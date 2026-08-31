import { HttpStatus } from '@nestjs/common';

import { JobStatus } from '@libs/core/workflow/domain/enums/job-status.enum';
import { WorkflowQueueController } from './workflow-queue.controller';

describe('WorkflowQueueController', () => {
    it('cancels a job for the authenticated organization', async () => {
        const job = {
            id: 'job-1',
            status: JobStatus.CANCELLED,
        };
        const jobStatusService = {
            cancelJob: jest.fn().mockResolvedValue({ job, cancelled: true }),
        };
        const controller = new WorkflowQueueController(
            jobStatusService as any,
            { user: { organization: { uuid: 'org-1' } } } as any,
        );

        await expect(controller.cancelJob('job-1')).resolves.toEqual({
            status: HttpStatus.OK,
            data: job,
        });
        expect(jobStatusService.cancelJob).toHaveBeenCalledWith(
            'job-1',
            'org-1',
        );
    });

    it('returns conflict for a terminal job', async () => {
        const jobStatusService = {
            cancelJob: jest.fn().mockResolvedValue({
                job: { status: JobStatus.COMPLETED },
                cancelled: false,
            }),
        };
        const controller = new WorkflowQueueController(
            jobStatusService as any,
            { user: { organization: { uuid: 'org-1' } } } as any,
        );

        await expect(controller.cancelJob('job-1')).resolves.toEqual({
            status: HttpStatus.CONFLICT,
            message: 'Job is already COMPLETED',
            data: { status: JobStatus.COMPLETED },
        });
    });
});
