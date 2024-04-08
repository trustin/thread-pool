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

    private final Terminator terminator;
    private final Watchdog watchdog;

    AbstractThreadPool(long taskTimeoutNanos, long watchdogIntervalNanos) {
        terminator = new Terminator();
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
        doShutdown(false);
    }

    @Override
    public final List<Runnable> shutdownNow() {
        doShutdown(true);
        return drainUnprocessedTasks();
    }

    private void doShutdown(boolean interrupt) {
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
            return;
        }

        final boolean hasWorkers = terminateWorkers();
        if (hasWorkers || (watchdog != null && watchdog.isStarted())) {
            terminator.start();
        } else {
            setTerminated();
        }
    }

    abstract List<Runnable> drainUnprocessedTasks();

    abstract boolean hasWorkers();

    abstract void forEachWorker(Consumer<ManagedThread<?>> block);

    abstract boolean terminateWorkers();

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

    final boolean needsToInterruptWorkers() {
        return state.get() == ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS;
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


    private final class Terminator extends ManagedThread<Void> {

        Terminator() {
            super(PlatformThreadPool.threadFactory);
        }

        @Override
        @Nullable
        Void call() {
            // Terminate all task workers.
            terminateWorkers();

            // Terminate the watchdog last so that it interrupts the long-running tasks even during shutdown.
            terminate(watchdog, "watchdog");

            // Let `ThreadPool` update its state and notify its listeners.
            setTerminated();

            return null;
        }

        private void terminate(@Nullable ManagedThread<?> thread, String friendlyName) {
            if (thread == null) {
                return;
            }

            logger.debug("Terminating the {} ..", friendlyName);
            thread.cancel(true);
            thread.joinThread();
            logger.debug("Terminated the {}.", friendlyName);
        }

        private void terminateWorkers() {
            if (hasWorkers()) {
                logger.debug("Terminating all workers ..");
                do {
                    // Interrupt all workers if needed.
                    final boolean interrupt = needsToInterruptWorkers();
                    forEachWorker(w -> w.cancel(interrupt));

                    // Wait for all workers to stop.
                    forEachWorker(ManagedThread::joinThread);
                } while (hasWorkers());
                logger.debug("Terminated all workers.");
            }
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

                    forEachWorker(w -> {
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
