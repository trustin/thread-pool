package threadpool;

import java.util.concurrent.Delayed;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
abstract class ManagedThread<T> extends CompletableScheduledFuture<T> implements RunnableScheduledFuture<T> {

    static final long UNSPECIFIED = 0;

    private final Thread thread;
    private final AtomicBoolean started = new AtomicBoolean();

    private long lastActivityTimeNanos = UNSPECIFIED;

    ManagedThread(ThreadFactory factory) {
        thread = factory.newThread(() -> {
            try {
                if (isDone()) {
                    return;
                }
                updateLastActivityTimeNanos();
                doComplete(call());
            } catch (Throwable cause) {
                doCompleteExceptionally(cause);
            }
        });
    }

    @Nullable
    abstract T call() throws Exception;

    @Override
    public final void run() {
        throw new UnsupportedOperationException();
    }

    final void start() {
        if (started.compareAndSet(false, true)) {
            thread.start();
        }
    }

    final boolean isStarted() {
        return started.get();
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
    public boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = super.cancel(false);
        if (!cancelled) {
            return false;
        }

        if (mayInterruptIfRunning) {
            interrupt();
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
    private void doComplete(T value) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        super.complete(value);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void doCompleteExceptionally(Throwable cause) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        super.completeExceptionally(cause);
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
