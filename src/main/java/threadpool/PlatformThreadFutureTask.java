package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import io.micrometer.core.instrument.Timer;

final class PlatformThreadFutureTask<V> extends CompletableFuture<V> implements RunnableFuture<V> {

    private final Callable<V> task;
    @Nullable
    private final Timer taskWaitTimer;
    private final long taskWaitStartTimeNanos;

    PlatformThreadFutureTask(Runnable task, @Nullable V result, @Nullable Timer taskWaitTimer) {
        this(Executors.callable(task, result), taskWaitTimer);
    }

    PlatformThreadFutureTask(Callable<V> task, @Nullable Timer taskWaitTimer) {
        this.task = task;
        this.taskWaitTimer = taskWaitTimer;
        if (taskWaitTimer != null) {
            taskWaitStartTimeNanos = System.nanoTime();
        } else {
            taskWaitStartTimeNanos = 0;
        }
    }

    @Override
    public void run() {
        if (isDone()) {
            return;
        }

        try {
            recordWaitTime();
            super.complete(task.call());
        } catch (Throwable e) {
            super.completeExceptionally(e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (cancelled) {
            recordWaitTime();
        }
        return cancelled;
    }

    private void recordWaitTime() {
        if (taskWaitTimer != null) {
            taskWaitTimer.record(System.nanoTime() - taskWaitStartTimeNanos, TimeUnit.NANOSECONDS);
        }
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
