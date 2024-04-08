package threadpool;

import javax.annotation.Nullable;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;

public final class ThreadPoolMetrics {

    private final Gauge state;
    private final Gauge workers;
    @Nullable
    private final Gauge scheduledTaskWorkers;
    private final Gauge busyWorkers;
    @Nullable
    private final Gauge busyScheduledTaskWorkers;
    private final Gauge minWorkers;
    @Nullable
    private final Gauge minScheduledTaskWorkers;
    private final Gauge maxWorkers;
    @Nullable
    private final Gauge maxScheduledTaskWorkers;
    private final Gauge taskQueueSize;
    @Nullable
    private final Gauge scheduledTaskQueueSize;
    private final Timer successfulTaskExecutionTimer;
    private final Timer failedTaskExecutionTimer;
    private final Timer taskWaitTimer;
    private final Gauge scheduledTasks;

    public ThreadPoolMetrics(Gauge state,
                             Gauge workers, @Nullable Gauge scheduledTaskWorkers,
                             Gauge busyWorkers, @Nullable Gauge busyScheduledTaskWorkers,
                             Gauge minWorkers, @Nullable Gauge minScheduledTaskWorkers,
                             Gauge maxWorkers, @Nullable Gauge maxScheduledTaskWorkers,
                             Gauge taskQueueSize, @Nullable Gauge scheduledTaskQueueSize,
                             Timer successfulTaskExecutionTimer,
                             Timer failedTaskExecutionTimer,
                             Timer taskWaitTimer,
                             Gauge scheduledTasks) {
        this.state = state;
        this.workers = workers;
        this.scheduledTaskWorkers = scheduledTaskWorkers;
        this.busyWorkers = busyWorkers;
        this.busyScheduledTaskWorkers = busyScheduledTaskWorkers;
        this.minWorkers = minWorkers;
        this.minScheduledTaskWorkers = minScheduledTaskWorkers;
        this.maxWorkers = maxWorkers;
        this.maxScheduledTaskWorkers = maxScheduledTaskWorkers;
        this.taskQueueSize = taskQueueSize;
        this.scheduledTaskQueueSize = scheduledTaskQueueSize;
        this.successfulTaskExecutionTimer = successfulTaskExecutionTimer;
        this.failedTaskExecutionTimer = failedTaskExecutionTimer;
        this.taskWaitTimer = taskWaitTimer;
        this.scheduledTasks = scheduledTasks;
    }

    public Gauge state() {
        return state;
    }

    public Gauge workers() {
        return workers;
    }

    @Nullable
    public Gauge scheduledTaskWorkers() {
        return scheduledTaskWorkers;
    }

    public Gauge busyWorkers() {
        return busyWorkers;
    }

    @Nullable
    public Gauge busyScheduledTaskWorkers() {
        return busyScheduledTaskWorkers;
    }

    public Gauge minWorkers() {
        return minWorkers;
    }

    @Nullable
    public Gauge minScheduledTaskWorkers() {
        return minScheduledTaskWorkers;
    }

    public Gauge maxWorkers() {
        return maxWorkers;
    }

    @Nullable
    public Gauge maxScheduledTaskWorkers() {
        return maxScheduledTaskWorkers;
    }

    public Gauge taskQueueSize() {
        return taskQueueSize;
    }

    @Nullable
    public Gauge scheduledTaskQueueSize() {
        return scheduledTaskQueueSize;
    }

    public Timer successfulTaskExecutionTimer() {
        return successfulTaskExecutionTimer;
    }

    public Timer failedTaskExecutionTimer() {
        return failedTaskExecutionTimer;
    }

    public Timer taskWaitTimer() {
        return taskWaitTimer;
    }

    public Gauge scheduledTasks() {
        return scheduledTasks;
    }
}
