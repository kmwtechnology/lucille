export interface Run {
  runId: string;
  configId: string;
  startTime: number | null;
  endTime: number | null;
  runResult: unknown;
  runType: string;
  done: boolean;
  future?: {
    done?: boolean;
    cancelled?: boolean;
    completedExceptionally?: boolean;
    numberOfDependents?: number;
  };
}
