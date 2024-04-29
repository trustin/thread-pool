package threadpool;

import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
abstract class ManagedThread<T> extends CompletableScheduledFuture<T> implements RunnableScheduledFuture<T> {

    enum State {
        CREATED,
        STARTED,
        PROCESSED,
        UNPROCESSED
    }

    static final long UNSPECIFIED = 0;

    private final Thread thread;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    private long lastActivityTimeNanos = UNSPECIFIED;

    ManagedThread(ThreadFactory factory) {
        thread = factory.newThread(() -> {
            try {
                if (isDone()) {
                    return;
                }
                doComplete(call());
            } catch (Throwable cause) {
                doCompleteExceptionally(cause);
            } finally {
                onStop();
            }
        });
    }

    final void start() {
        if (state.compareAndSet(State.CREATED, State.STARTED)) {
            onStart();
            thread.start();
        }
    }

    final boolean isStarted() {
        return state.get() != State.CREATED;
    }

    final boolean setProcessed() {
        return state.compareAndSet(State.STARTED, State.PROCESSED);
    }

    private boolean setUnprocessed() {
        return state.compareAndSet(State.STARTED, State.UNPROCESSED);
    }

    void onStart() {}

    void onStop() {}

    @Nullable
    abstract T call() throws Exception;

    @Override
    public final void run() {
        throw new UnsupportedOperationException();
    }

    final String threadName() {
        return thread.getName();
    }

    final void interrupt() {
        thread.interrupt();
    }

    final void joinThread() {
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = super.cancel(false);
        if (!cancelled) {
            return false;
        }

        if (mayInterruptIfRunning) {
            interrupt();
        }
        return true;
    }

    final boolean cancelByShutdownNow(@Nullable List<Runnable> unprocessedTasks) {
        final boolean cancelled = cancel(true);
        if (!cancelled) {
            return false;
        }

        if (!setUnprocessed()) {
            return true;
        }

        if (unprocessedTasks != null) {
            unprocessedTasks.add(this);
        }

        return true;
    }

    final long lastActivityTimeNanos() {
        return lastActivityTimeNanos;
    }

    final long updateLastActivityTimeNanos() {
        final long currentTimeNanos = System.nanoTime();
        final long lastActivityTimeNanos = currentTimeNanos != 0 ? currentTimeNanos : 1;
        this.lastActivityTimeNanos = lastActivityTimeNanos;
        return lastActivityTimeNanos;
    }

    final void clearLastActivityTimeNanos() {
        lastActivityTimeNanos = UNSPECIFIED;
    }

    @Override
    public boolean isPeriodic() {
        return false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return -1;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private boolean doComplete(@Nullable T value) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        return super.complete(value);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private boolean doCompleteExceptionally(Throwable cause) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        return super.completeExceptionally(cause);
    }

    @Override
    public final boolean complete(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void obtrudeValue(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Delayed o) {
        throw new UnsupportedOperationException();
    }
}
