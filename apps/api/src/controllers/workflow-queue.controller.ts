import {
    Controller,
    Get,
    Param,
    Post,
    UseGuards,
    HttpStatus,
    HttpCode,
    UnauthorizedException,
} from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { REQUEST } from '@nestjs/core';

import {
    Action,
    ResourceType,
} from '@libs/identity/domain/permissions/enums/permissions.enum';
import { PolicyGuard } from '@libs/identity/infrastructure/adapters/services/permissions/policy.guard';
import { CheckPolicies } from '@libs/identity/infrastructure/adapters/services/permissions/policy.guard';
import { checkPermissions } from '@libs/identity/infrastructure/adapters/services/permissions/policy.handlers';
import { JOB_STATUS_SERVICE_TOKEN } from '@libs/core/workflow/domain/contracts/job-status.service.contract';
import { IJobStatusService } from '@libs/core/workflow/domain/contracts/job-status.service.contract';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { ApiStandardResponses } from '../docs/api-standard-responses.decorator';
import { UserRequest } from '@libs/core/infrastructure/config/types/http/user-request.type';

@ApiTags('Workflow Queue')
@ApiBearerAuth('jwt')
@ApiStandardResponses()
@Controller('workflow-queue')
@UseGuards(PolicyGuard)
export class WorkflowQueueController {
    constructor(
        @Inject(JOB_STATUS_SERVICE_TOKEN)
        private readonly jobStatusService: IJobStatusService,
        @Inject(REQUEST)
        private readonly request: UserRequest,
    ) {}

    @Get('/jobs/:jobId')
    @CheckPolicies(
        checkPermissions({
            action: Action.Read,
            resource: ResourceType.CodeReviewSettings,
        }),
    )
    @HttpCode(HttpStatus.OK)
    async getJobStatus(@Param('jobId') jobId: string) {
        const job = await this.jobStatusService.getJobStatus(jobId);

        if (!job) {
            return {
                status: HttpStatus.NOT_FOUND,
                message: 'Job not found',
            };
        }

        return {
            status: HttpStatus.OK,
            data: job,
        };
    }

    @Post('/jobs/:jobId/cancel')
    @CheckPolicies(
        checkPermissions({
            action: Action.Read,
            resource: ResourceType.CodeReviewSettings,
        }),
    )
    @HttpCode(HttpStatus.OK)
    async cancelJob(@Param('jobId') jobId: string) {
        const organizationId = this.request.user?.organization?.uuid;
        if (!organizationId) {
            throw new UnauthorizedException(
                'Authenticated organization not found',
            );
        }

        const result = await this.jobStatusService.cancelJob(
            jobId,
            organizationId,
        );

        if (!result) {
            return {
                status: HttpStatus.NOT_FOUND,
                message: 'Job not found',
            };
        }

        if (!result.cancelled) {
            return {
                status: HttpStatus.CONFLICT,
                message: `Job is already ${result.job.status}`,
                data: result.job,
            };
        }

        return {
            status: HttpStatus.OK,
            data: result.job,
        };
    }

    @Get('/jobs/:jobId/detail')
    @CheckPolicies(
        checkPermissions({
            action: Action.Read,
            resource: ResourceType.CodeReviewSettings,
        }),
    )
    @HttpCode(HttpStatus.OK)
    async getJobDetail(@Param('jobId') jobId: string) {
        const detail = await this.jobStatusService.getJobDetail(jobId);

        if (!detail) {
            return {
                status: HttpStatus.NOT_FOUND,
                message: 'Job not found',
            };
        }

        return {
            status: HttpStatus.OK,
            data: detail,
        };
    }

    @Get('/metrics')
    @CheckPolicies(
        checkPermissions({
            action: Action.Read,
            resource: ResourceType.CodeReviewSettings,
        }),
    )
    @HttpCode(HttpStatus.OK)
    async getMetrics() {
        const metrics = await this.jobStatusService.getMetrics();

        return {
            status: HttpStatus.OK,
            data: metrics,
        };
    }
}
