package threadpool;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public final class ThreadPoolBuilder extends AbstractThreadPoolBuilder<ThreadPoolBuilder> {

    private final int maxNumWorkers;
    private int minNumWorkers;
    private long idleTimeoutNanos;
    private boolean scheduledTaskWorkersEnabled;
    private int maxNumScheduledTaskWorkers;
    private int minNumScheduledTaskWorkers;
    private Supplier<BlockingQueue<Runnable>> taskQueueFactory = LinkedBlockingQueue::new;

    ThreadPoolBuilder(int maxNumWorkers) {
        this.maxNumWorkers = checkMaxNumWorkers("maxNumWorkers", maxNumWorkers);
    }

    public ThreadPoolBuilder minNumWorkers(int minNumWorkers) {
        this.minNumWorkers = checkMinNumWorkers("minNumWorkers", minNumWorkers, "maxNumWorkers", maxNumWorkers);
        return this;
    }

    public ThreadPoolBuilder idleTimeout(long idleTimeout, TimeUnit unit) {
        checkArgument(idleTimeout >= 0, "idleTimeout: %s (expected: >= 0)");
        idleTimeoutNanos = requireNonNull(unit, "unit").toNanos(idleTimeout);
        return this;
    }

    public ThreadPoolBuilder idleTimeout(Duration idleTimeout) {
        requireNonNull(idleTimeout, "idleTimeout");
        checkArgument(!idleTimeout.isNegative(), "idleTimeout: %s (expected: >= 0)");
        return idleTimeout(idleTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public ThreadPoolBuilder taskQueue(Supplier<? extends BlockingQueue<Runnable>> taskQueueFactory) {
        @SuppressWarnings("unchecked")
        final Supplier<BlockingQueue<Runnable>> cast =
                (Supplier<BlockingQueue<Runnable>>) requireNonNull(taskQueueFactory, "taskQueueFactory");
        this.taskQueueFactory = cast;
        return this;
    }

    public ThreadPoolBuilder scheduledTaskWorkers(int maxNumScheduledTaskWorkers) {
        return scheduledTaskWorkers(maxNumScheduledTaskWorkers, 0);
    }

    public ThreadPoolBuilder scheduledTaskWorkers(int maxNumScheduledTaskWorkers,
                                                  int minNumScheduledTaskWorkers) {
        checkMaxNumWorkers("maxNumScheduledTaskWorkers", maxNumScheduledTaskWorkers);
        checkMinNumWorkers("minNumScheduledTaskWorkers", minNumScheduledTaskWorkers,
                           "maxNumScheduledTaskWorkers", maxNumScheduledTaskWorkers);

        scheduledTaskWorkersEnabled = true;
        this.maxNumScheduledTaskWorkers = maxNumScheduledTaskWorkers;
        this.minNumScheduledTaskWorkers = minNumScheduledTaskWorkers;
        return this;
    }

    private static int checkMaxNumWorkers(String paramName, int maxNumWorkers) {
        checkArgument(maxNumWorkers > 0, "%s: %s (expected: > 0)", paramName, maxNumWorkers);
        return maxNumWorkers;
    }

    private static int checkMinNumWorkers(String minNumWorkersName, int minNumWorkers,
                                          String maxNumWorkersName, int maxNumWorkers) {
        checkArgument(minNumWorkers >= 0 && minNumWorkers <= maxNumWorkers,
                      "%s: %s (expected: [0, %s (%s)]",
                      minNumWorkersName, minNumWorkers,
                      maxNumWorkersName, maxNumWorkers);
        return minNumWorkers;
    }

    @Override
    ThreadPool build(TaskSubmissionHandler submissionHandler, TaskExceptionHandler exceptionHandler,
                     long taskTimeoutNanos, long watchdogIntervalNanos, MeterRegistry meterRegistry,
                     String metricPrefix, List<Tag> metricTags) {

        final BlockingQueue<Runnable> taskQueue = taskQueueFactory.get();
        final PlatformWorkerGroup workers = new PlatformWorkerGroup(
                minNumWorkers, maxNumWorkers,
                idleTimeoutNanos, taskQueue,
                submissionHandler, exceptionHandler);

        final PlatformWorkerGroup scheduledWorkers;
        if (scheduledTaskWorkersEnabled) {
            final BlockingQueue<Runnable> scheduledTaskQueue = taskQueueFactory.get();
            scheduledWorkers = new PlatformWorkerGroup(
                    minNumScheduledTaskWorkers,
                    maxNumScheduledTaskWorkers,
                    idleTimeoutNanos, scheduledTaskQueue,
                    submissionHandler, exceptionHandler);
        } else {
            scheduledWorkers = null;
        }

        final PlatformThreadPool pool;
        if (scheduledTaskWorkersEnabled) {
            pool = new PlatformThreadPool(workers, scheduledWorkers,
                                          taskTimeoutNanos, watchdogIntervalNanos,
                                          meterRegistry, metricPrefix, metricTags);
        } else {
            pool = new PlatformThreadPool(workers, workers,
                                          taskTimeoutNanos, watchdogIntervalNanos,
                                          meterRegistry, metricPrefix, metricTags);
        }

        workers.setParent(pool);
        if (scheduledWorkers != null) {
            scheduledWorkers.setParent(pool);
        }

        return pool;
    }
}
