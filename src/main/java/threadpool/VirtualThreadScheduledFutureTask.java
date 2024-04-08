package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import javax.annotation.Nullable;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class VirtualThreadScheduledFutureTask<V> extends ManagedThread<V> {

    private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

    private static final AtomicInteger differentiatorGenerator = new AtomicInteger();

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<VirtualThreadScheduledFutureTask> differentiatorUpdater =
            AtomicIntegerFieldUpdater.newUpdater(VirtualThreadScheduledFutureTask.class, "differentiator");

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

    VirtualThreadScheduledFutureTask(Runnable task, @Nullable V result,
                                     long initialDelayNanos, long periodNanos) {
        super(VirtualThreadPool.threadFactory);
        this.task = Executors.callable(task, result);
        deadlineNanos = System.nanoTime() + initialDelayNanos;
        this.periodNanos = periodNanos;
        // A negative periodNanos value is always negated from a positive long value.
        assert periodNanos != Long.MIN_VALUE;
    }

    VirtualThreadScheduledFutureTask(Callable<V> task, long delayNanos) {
        super(VirtualThreadPool.threadFactory);
        this.task = task;
        deadlineNanos = System.nanoTime() + delayNanos;
        periodNanos = 0; // There are no periodic `schedule*()` methods that accept a `Callable`.
    }

    @Override
    @Nullable
    V call() throws Exception {
        long delayNanos = getDelay(TimeUnit.NANOSECONDS);
        if (isPeriodic()) {
            for (;;) {
                delay(delayNanos);
                task.call();
                delayNanos = updateDeadlineAndGetDelayNanos();
            }
        } else {
            delay(delayNanos);
            return task.call();
        }
    }

    private static void delay(long delayNanos) throws InterruptedException {
        if (delayNanos > 0) {
            Thread.sleep(delayNanos / NANOS_PER_MILLI, (int) (delayNanos % NANOS_PER_MILLI));
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

    long updateDeadlineAndGetDelayNanos() {
        final long currentTimeNanos = System.nanoTime();
        if (periodNanos > 0) { // Run at fixed rate.
            deadlineNanos += periodNanos;
            return deadlineNanos - currentTimeNanos;
        } else { // Run with fixed delay.
            final long delayNanos = -periodNanos;
            // periodNanos is negated from a positive long value, so it is never Long.MIN_VALUE.
            assert delayNanos > 0;
            deadlineNanos = currentTimeNanos + delayNanos;
            return delayNanos;
        }
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

        final VirtualThreadScheduledFutureTask<?> that = (VirtualThreadScheduledFutureTask<?>) o;
        return ThreadPoolUtil.compareScheduledTasks(this, deadlineNanos, that, that.deadlineNanos,
                                                    differentiatorUpdater, differentiatorGenerator);
    }
}
