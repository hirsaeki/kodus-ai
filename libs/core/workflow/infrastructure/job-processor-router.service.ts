import { Injectable, Inject } from '@nestjs/common';
import { createLogger } from '@libs/core/log/logger';

import { runWithTimeout } from './run-with-timeout';
import { IJobProcessorRouter } from '@libs/core/workflow/domain/contracts/job-processor-router.contract';
import { IJobProcessorService } from '@libs/core/workflow/domain/contracts/job-processor.service.contract';
import {
    IWorkflowJobRepository,
    WORKFLOW_JOB_REPOSITORY_TOKEN,
} from '@libs/core/workflow/domain/contracts/workflow-job.repository.contract';
import { WorkflowType } from '@libs/core/workflow/domain/enums/workflow-type.enum';
import { JobStatus } from '@libs/core/workflow/domain/enums/job-status.enum';
import { ErrorClassification } from '@libs/core/workflow/domain/enums/error-classification.enum';
import {
    isJobCancelledError,
    JobCancelledError,
} from '@libs/core/workflow/domain/errors/job-cancelled.error';

import { WebhookProcessingJobProcessorService } from '@libs/automation/webhook-processing/webhook-processing-job.processor';
import { CodeReviewJobProcessorService } from '@libs/code-review/workflow/code-review-job-processor.service';

import { ImplementationVerificationProcessor } from '@libs/code-review/workflow/implementation-verification.processor';
import { AstGraphBuildJobProcessor } from '@libs/code-review/workflow/ast-graph-build-job.processor';
import { AstGraphIncrementalJobProcessor } from '@libs/code-review/workflow/ast-graph-incremental-job.processor';
import { CliReviewJobProcessorService } from '@libs/cli-review/workflow/cli-review-job-processor.service';

// App-level timeouts MUST be strictly less than the broker's consumer_timeout
// (7200000ms / 2h in prod — see envs/aws/prod/rabbitmq-ec2.tfvars in kodus-infra).
// The margin lets the app run its cleanup chain (catch → update FAILED →
// releaseLock → throw → errorHandler.republish → channel.ack) BEFORE the broker
// kills the channel, which would otherwise leave the message unacked and only
// freed on a physical worker restart (the symptom seen in 2026-05).
const WEBHOOK_PROCESS_TIMEOUT_MS = 9 * 60 * 1000; // 9 min (broker default 30min)
const CODE_REVIEW_PROCESS_TIMEOUT_MS = 105 * 60 * 1000; // 1h45min (broker 2h)
const CLI_CODE_REVIEW_PROCESS_TIMEOUT_MS = 28 * 60 * 1000; // 28 min
const CHECK_IMPLEMENTATION_TIMEOUT_MS = 9 * 60 * 1000; // 9 min
const AST_GRAPH_BUILD_TIMEOUT_MS = 19 * 60 * 1000; // 19 min
const AST_GRAPH_INCREMENTAL_TIMEOUT_MS = 9 * 60 * 1000; // 9 min
const JOB_CANCELLATION_POLL_MS = 2_000;

@Injectable()
export class JobProcessorRouterService
    implements IJobProcessorService, IJobProcessorRouter
{
    private readonly logger = createLogger(JobProcessorRouterService.name);

    constructor(
        @Inject(WORKFLOW_JOB_REPOSITORY_TOKEN)
        private readonly jobRepository: IWorkflowJobRepository,
        private readonly codeReviewProcessor: CodeReviewJobProcessorService,
        private readonly webhookProcessor: WebhookProcessingJobProcessorService,
        private readonly implementationVerificationProcessor: ImplementationVerificationProcessor,
        private readonly astGraphBuildProcessor: AstGraphBuildJobProcessor,
        private readonly astGraphIncrementalProcessor: AstGraphIncrementalJobProcessor,
        private readonly cliReviewProcessor: CliReviewJobProcessorService,
    ) {}

    async process(jobId: string): Promise<void> {
        const job = await this.jobRepository.findOne(jobId);

        if (!job) {
            throw new Error(`Workflow job ${jobId} not found`);
        }

        if (job.status === JobStatus.CANCELLED) {
            throw new JobCancelledError(jobId);
        }

        const processor = this.getProcessor(job.workflowType);
        const timeoutMs = this.getProcessTimeoutMs(job.workflowType);
        const cancellationController = new AbortController();
        const cancellationPoll = setInterval(() => {
            void this.pollCancellation(jobId, cancellationController);
        }, JOB_CANCELLATION_POLL_MS);

        try {
            return await runWithTimeout(
                (signal) => processor.process(jobId, signal),
                timeoutMs,
                `Workflow job ${jobId} timeout after ${timeoutMs}ms`,
                cancellationController.signal,
            );
        } catch (rawError) {
            const error =
                rawError instanceof Error
                    ? rawError
                    : new Error(String(rawError));

            if (await this.wasCancelled(jobId, error)) {
                throw new JobCancelledError(jobId);
            }

            const isTimeout = error.message?.includes('timeout after');

            // Always mark job as FAILED when an error occurs (including timeout)
            try {
                await this.jobRepository.update(jobId, {
                    status: JobStatus.FAILED,
                    errorClassification: isTimeout
                        ? ErrorClassification.RETRYABLE
                        : ErrorClassification.PERMANENT,
                    lastError: error.message,
                });

                this.logger.error({
                    message: `Job ${jobId} marked as FAILED${isTimeout ? ' due to timeout' : ''}`,
                    context: JobProcessorRouterService.name,
                    error,
                    metadata: {
                        jobId,
                        workflowType: job.workflowType,
                        isTimeout,
                        timeoutMs,
                    },
                });
            } catch (updateError) {
                this.logger.error({
                    message: `Failed to update job ${jobId} status to FAILED`,
                    context: JobProcessorRouterService.name,
                    error: updateError,
                    metadata: { jobId, originalError: error.message },
                });
            }

            throw error;
        } finally {
            clearInterval(cancellationPoll);
        }
    }

    private async pollCancellation(
        jobId: string,
        controller: AbortController,
    ): Promise<void> {
        if (controller.signal.aborted) return;

        try {
            const job = await this.jobRepository.findOne(jobId);
            if (job?.status === JobStatus.CANCELLED) {
                controller.abort(new JobCancelledError(jobId));
                this.logger.log({
                    message: `Cancellation detected for workflow job ${jobId}`,
                    context: JobProcessorRouterService.name,
                    metadata: { jobId },
                });
            }
        } catch (error) {
            this.logger.warn({
                message: `Failed to poll cancellation for workflow job ${jobId}`,
                context: JobProcessorRouterService.name,
                error,
                metadata: { jobId },
            });
        }
    }

    private async wasCancelled(jobId: string, error: Error): Promise<boolean> {
        if (isJobCancelledError(error)) return true;

        try {
            return (
                (await this.jobRepository.findOne(jobId))?.status ===
                JobStatus.CANCELLED
            );
        } catch {
            return false;
        }
    }

    async handleFailure(jobId: string, error: Error): Promise<void> {
        const job = await this.jobRepository.findOne(jobId);

        if (!job) {
            throw new Error(`Workflow job ${jobId} not found`);
        }

        const processor = this.getProcessor(job.workflowType);
        return await processor.handleFailure(jobId, error);
    }

    async markCompleted(jobId: string, result?: unknown): Promise<void> {
        const job = await this.jobRepository.findOne(jobId);

        if (!job) {
            throw new Error(`Workflow job ${jobId} not found`);
        }

        const processor = this.getProcessor(job.workflowType);
        return await processor.markCompleted(jobId, result);
    }

    private getProcessor(workflowType: WorkflowType): IJobProcessorService {
        switch (workflowType) {
            case WorkflowType.WEBHOOK_PROCESSING:
                return this.webhookProcessor;
            case WorkflowType.CODE_REVIEW:
                return this.codeReviewProcessor;
            case WorkflowType.CLI_CODE_REVIEW:
                return this.cliReviewProcessor;
            case WorkflowType.CHECK_SUGGESTION_IMPLEMENTATION:
                return this.implementationVerificationProcessor;
            case WorkflowType.AST_GRAPH_BUILD:
                return this.astGraphBuildProcessor;
            case WorkflowType.AST_GRAPH_INCREMENTAL:
                return this.astGraphIncrementalProcessor;
            default:
                throw new Error(
                    `No processor found for workflow type: ${workflowType}`,
                );
        }
    }

    private getProcessTimeoutMs(workflowType: WorkflowType): number {
        switch (workflowType) {
            case WorkflowType.WEBHOOK_PROCESSING:
                return WEBHOOK_PROCESS_TIMEOUT_MS;
            case WorkflowType.CODE_REVIEW:
                return CODE_REVIEW_PROCESS_TIMEOUT_MS;
            case WorkflowType.CLI_CODE_REVIEW:
                return CLI_CODE_REVIEW_PROCESS_TIMEOUT_MS;
            case WorkflowType.CHECK_SUGGESTION_IMPLEMENTATION:
                return CHECK_IMPLEMENTATION_TIMEOUT_MS;
            case WorkflowType.AST_GRAPH_BUILD:
                return AST_GRAPH_BUILD_TIMEOUT_MS;
            case WorkflowType.AST_GRAPH_INCREMENTAL:
                return AST_GRAPH_INCREMENTAL_TIMEOUT_MS;
            default:
                return CODE_REVIEW_PROCESS_TIMEOUT_MS;
        }
    }
}
