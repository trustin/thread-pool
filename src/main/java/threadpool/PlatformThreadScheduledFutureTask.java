package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import javax.annotation.Nullable;

import io.micrometer.core.instrument.Timer;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class PlatformThreadScheduledFutureTask<V>
        extends CompletableScheduledFuture<V>
        implements RunnableScheduledFuture<V> {

    private static final AtomicInteger differentiatorGenerator = new AtomicInteger();

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<PlatformThreadScheduledFutureTask> differentiatorUpdater =
            AtomicIntegerFieldUpdater.newUpdater(PlatformThreadScheduledFutureTask.class, "differentiator");

    private static final int NOT_SCHEDULED = -1;

    private static final long TASK_NOT_WAITING_MARKER = 0;

    private final Callable<V> task;

    /**
     * If positive, the task is run at fixed rate.
     * If negative, the task is run with fixed delay.
     * If zero, the task is not periodic.
     */
    private final long periodNanos;
    private long deadlineNanos;

    /**
     * A value that is used as the last resort for comparison in {@link #compareTo(Delayed)}.
     * This field is set lazily only when {@link #compareTo(Delayed)} found two objects with the same
     * {@linkplain System#identityHashCode(Object) identity hash code}. {@literal 0} means that this field is
     * not set yet.
     * 
     * @see ThreadPoolUtil#compareScheduledTasks(Object, long, Object, long, AtomicIntegerFieldUpdater, AtomicInteger)
     */
    @SuppressWarnings("unused")
    private volatile int differentiator;

    private final PlatformThreadPool.Scheduler scheduler;

    private int schedulerIndex = NOT_SCHEDULED;

    @Nullable
    private final Timer taskWaitTimer;
    private volatile long taskWaitStartTimeNanos = TASK_NOT_WAITING_MARKER;

    PlatformThreadScheduledFutureTask(PlatformThreadPool.Scheduler scheduler, Runnable task, @Nullable V result,
                                      long initialDelayNanos, long periodNanos,
                                      @Nullable Timer taskWaitTimer) {
        this.task = Executors.callable(task, result);
        deadlineNanos = System.nanoTime() + initialDelayNanos;
        this.periodNanos = periodNanos;
        // A negative periodNanos value is always negated from a positive long value.
        assert periodNanos != Long.MIN_VALUE;

        this.scheduler = scheduler;
        this.taskWaitTimer = taskWaitTimer;
    }

    PlatformThreadScheduledFutureTask(PlatformThreadPool.Scheduler scheduler, Callable<V> task,
                                      long delayNanos, @Nullable Timer taskWaitTimer) {
        this.task = task;
        deadlineNanos = System.nanoTime() + delayNanos;
        periodNanos = 0; // There are no periodic `schedule*()` methods that accept a `Callable`.

        this.scheduler = scheduler;
        this.taskWaitTimer = taskWaitTimer;
    }

    @Override
    public void run() {
        if (isDone()) {
            return;
        }

        try {
            recordWaitTime();
            if (isPeriodic()) {
                task.call();
                updateDeadline();
                scheduler.reschedule(this);
            } else {
                final V result = task.call();
                if (super.complete(result)) {
                    scheduler.numScheduledTasks.decrement();
                }
            }
        } catch (Throwable cause) {
            if (super.completeExceptionally(cause)) {
                scheduler.numScheduledTasks.decrement();
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (cancelled) {
            recordWaitTime();
            scheduler.numScheduledTasks.decrement();
            if (isScheduled()) {
                scheduler.removeScheduledTask(this);
            }
        }
        return cancelled;
    }

    boolean isScheduled() {
        return schedulerIndex != NOT_SCHEDULED;
    }

    void setScheduled(int schedulerIndex) {
        assert schedulerIndex >= 0 : schedulerIndex;
        this.schedulerIndex = schedulerIndex;
    }

    void ensureUnscheduled() {
        assert schedulerIndex == NOT_SCHEDULED : schedulerIndex;
    }

    void setUnscheduled() {
        assert schedulerIndex != NOT_SCHEDULED;
        schedulerIndex = NOT_SCHEDULED;
    }

    int schedulerIndex() {
        assert schedulerIndex != NOT_SCHEDULED;
        return schedulerIndex;
    }

    void markWaitTimeStart() {
        if (taskWaitTimer != null) {
            final long nanoTime = System.nanoTime();
            taskWaitStartTimeNanos = nanoTime != TASK_NOT_WAITING_MARKER ? nanoTime : 1;
        }
    }

    private void recordWaitTime() {
        if (taskWaitTimer != null) {
            final long taskWaitStartTimeNanos = this.taskWaitStartTimeNanos;
            if (taskWaitStartTimeNanos != TASK_NOT_WAITING_MARKER) {
                taskWaitTimer.record(System.nanoTime() - taskWaitStartTimeNanos, TimeUnit.NANOSECONDS);
                this.taskWaitStartTimeNanos = TASK_NOT_WAITING_MARKER;
            }
        }
    }


    @Override
    public boolean isPeriodic() {
        return periodNanos != 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    void updateDeadline() {
        if (periodNanos > 0) { // Run at fixed rate.
            deadlineNanos += periodNanos;
        } else { // Run with fixed delay.
            deadlineNanos = nextDeadlineNanos();
        }
    }

    private long nextDeadlineNanos() {
        final long delayNanos = -periodNanos;
        // periodNanos is negated from a positive long value, so it is never Long.MIN_VALUE.
        assert delayNanos > 0;
        return System.nanoTime() + delayNanos;
    }

    @Override
    public int compareTo(Delayed o) {
        if (o == this) {
            return 0;
        }

        if (getClass() != o.getClass()) {
            throw new IllegalArgumentException(
                    "cannot compare two different Delayed instances: " +
                    getClass().getName() + " vs. " +
                    o.getClass().getName());
        }

        final PlatformThreadScheduledFutureTask<?> that = (PlatformThreadScheduledFutureTask<?>) o;
        return ThreadPoolUtil.compareScheduledTasks(this, deadlineNanos, that, that.deadlineNanos,
                                                    differentiatorUpdater, differentiatorGenerator);
    }

    @Override
    public boolean complete(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void obtrudeValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException();
    }
}
