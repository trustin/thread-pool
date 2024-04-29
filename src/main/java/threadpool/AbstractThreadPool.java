package threadpool;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractThreadPool extends AbstractExecutorService implements ThreadPool {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private final AtomicReference<ThreadPoolState> state =
            new AtomicReference<>(ThreadPoolState.STARTED);

    private final TerminationWaiter terminationWaiter;
    private final Watchdog watchdog;

    AbstractThreadPool(long taskTimeoutNanos, long watchdogIntervalNanos) {
        terminationWaiter = new TerminationWaiter();
        watchdog = taskTimeoutNanos != 0 ? new Watchdog(taskTimeoutNanos, watchdogIntervalNanos)
                                         : null;
    }

    ThreadPoolState state() {
        return state.get();
    }

    final boolean enterState(ThreadPoolState oldState, ThreadPoolState newState) {
        return state.compareAndSet(oldState, newState);
    }

    @Nullable
    Watchdog watchdog() {
        return watchdog;
    }

    @Override
    public final void shutdown() {
        doShutdown(false, null);
    }

    final boolean doShutdown(boolean interrupt, @Nullable List<Runnable> unprocessedTasks) {
        boolean shutdownAlreadyInProgress = true;
        if (interrupt) {
            if (enterState(ThreadPoolState.STARTED,
                           ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS)) {
                shutdownAlreadyInProgress = false;
            } else {
                // needsShutdownTasks stays false because the shutdown tasks were added already
                // when shutdownState exits NOT_SHUTDOWN.
                enterState(ThreadPoolState.SHUT_DOWN_WITHOUT_INTERRUPTS,
                           ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS);
                logger.debug("shutdownNow() was called while shutdown() is in progress.");
            }
        } else {
            if (enterState(ThreadPoolState.STARTED,
                           ThreadPoolState.SHUT_DOWN_WITHOUT_INTERRUPTS)) {
                shutdownAlreadyInProgress = false;
            }
        }

        if (shutdownAlreadyInProgress) {
            return false;
        }

        sendPoisonPills();
        if (hasManagedThreads() || (watchdog != null && watchdog.isStarted())) {
            if (interrupt) {
                forEachManagedThread(thread -> thread.cancelByShutdownNow(unprocessedTasks));
            } else {
                forEachManagedThread(thread -> thread.cancel(false));
            }
            terminationWaiter.start();
        } else {
            setTerminated();
        }

        return true;
    }

    abstract boolean hasManagedThreads();

    abstract void forEachManagedThread(Consumer<ManagedThread<?>> block);

    abstract void forEachActiveManagedThread(Consumer<ManagedThread<?>> block);

    abstract void sendPoisonPills();

    @Override
    public final boolean isShutdown() {
        return state.get() != ThreadPoolState.STARTED;
    }

    @Override
    public final boolean isTerminated() {
        return terminationFuture.isDone();
    }

    @Override
    public final CompletableFuture<Void> terminationFuture() {
        return terminationFuture;
    }

    @Override
    public final void awaitTermination() throws InterruptedException {
        try {
            terminationFuture.get();
        } catch (ExecutionException e) {
            throw new Error("Should never reach here", e);
        }
    }

    @Override
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        requireNonNull(unit, "unit");
        try {
            terminationFuture.get(timeout, unit);
            return true;
        } catch (ExecutionException e) {
            throw new Error("Should never reach here", e);
        } catch (TimeoutException e) {
            return false;
        }
    }

    final void setTerminated() {
        state.set(ThreadPoolState.TERMINATED);
        terminationFuture.complete(null);
    }

    @Override
    public final CompletableFuture<?> submit(Runnable task) {
        return (CompletableFuture<?>) super.submit(task);
    }

    @Override
    public final <T> CompletableFuture<T> submit(Runnable task, T result) {
        return (CompletableFuture<T>) super.submit(task, result);
    }

    @Override
    public final <T> CompletableFuture<T> submit(Callable<T> task) {
        return (CompletableFuture<T>) super.submit(task);
    }


    private final class TerminationWaiter extends ManagedThread<Void> {

        TerminationWaiter() {
            super(PlatformThreadPool.threadFactory);
        }

        @Override
        @Nullable
        Void call() {
            // Join all managed threads.
            if (hasManagedThreads()) {
                logger.debug("Waiting for termination of all threads ..");
                do {
                    // Wait for all workers to stop.
                    forEachManagedThread(ManagedThread::joinThread);
                } while (hasManagedThreads());
                logger.debug("All threads have been terminated.");
            }

            // Terminate the watchdog last so that it interrupts the long-running tasks even during shutdown.
            if (watchdog != null) {
                logger.debug("Waiting for termination of watchdog ..");
                watchdog.cancel(true);
                watchdog.joinThread();
                logger.debug("Watchdog has been terminated.");
            }

            // Let `ThreadPool` update its state and notify its listeners.
            setTerminated();
            return null;
        }

    }

    final class Watchdog extends ManagedThread<Void> {
        private final long taskTimeoutNanos;
        private final long watchdogIntervalMillis;
        private final int watchdogIntervalRemainingNanos;

        Watchdog(long taskTimeoutNanos, long watchdogIntervalNanos) {
            super(PlatformThreadPool.threadFactory);
            this.taskTimeoutNanos = taskTimeoutNanos;
            final long nanosPerMilli = TimeUnit.MILLISECONDS.toNanos(1);
            watchdogIntervalMillis = watchdogIntervalNanos / nanosPerMilli;
            watchdogIntervalRemainingNanos = (int) (watchdogIntervalNanos % nanosPerMilli);
        }

        @Override
        @Nullable
        Void call() {
            logger.debug("Started a watchdog: {}", threadName());
            try {
                for (;;) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(watchdogIntervalMillis,
                                     watchdogIntervalRemainingNanos);
                    } catch (InterruptedException e) {
                        if (isShutdown()) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    forEachActiveManagedThread(w -> {
                        final long lastActivityTimeNanos = w.lastActivityTimeNanos();
                        if (lastActivityTimeNanos == UNSPECIFIED) {
                            return;
                        }
                        if (System.nanoTime() - lastActivityTimeNanos > taskTimeoutNanos) {
                            logger.debug("Interrupting a worker: {}", w.threadName());
                            w.interrupt();
                        }
                    });
                }
            } catch (Throwable cause) {
                logger.warn("Unexpected exception from a watchdog:", cause);
            } finally {
                logger.debug("A watchdog has been terminated: {}", threadName());
            }

            return null;
        }
    }
}
