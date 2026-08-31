export class JobCancelledError extends Error {
    constructor(jobId?: string) {
        super(
            jobId
                ? `Workflow job ${jobId} was cancelled`
                : 'Workflow job was cancelled',
        );
        this.name = 'JobCancelledError';
    }
}

export function isJobCancelledError(error: unknown): boolean {
    return (
        error instanceof JobCancelledError ||
        (error as { name?: string } | null)?.name === 'JobCancelledError'
    );
}

export function isJobCancellationSignal(
    signal: AbortSignal | undefined,
): boolean {
    return isJobCancelledError(signal?.reason);
}
