package threadpool;

import static threadpool.ThreadPoolUtil.handleLateSubmission;
import static threadpool.ThreadPoolUtil.invokeExceptionHandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import threadpool.AbstractThreadPool.Watchdog;

final class PlatformWorkerGroup {

    private static final Logger logger = LoggerFactory.getLogger(PlatformWorkerGroup.class);

    private static final Runnable[] EMPTY_RUNNABLES = new Runnable[0];

    private static final Worker[] EMPTY_WORKERS_ARRAY = new Worker[0];
    private static final Runnable SHUTDOWN_TASK = () -> {
    };

    private final int minNumWorkers;
    private final int maxNumWorkers;
    private final long idleTimeoutNanos;
    private final BlockingQueue<Runnable> taskQueue;
    private final TaskSubmissionHandler submissionHandler;
    private final TaskExceptionHandler exceptionHandler;

    private final Set<Worker> workers = new HashSet<>();
    private final Lock workersLock = new ReentrantLock();
    private final AtomicInteger numWorkers = new AtomicInteger();
    private final AtomicInteger numBusyWorkers = new AtomicInteger();
    @Nullable
    private PlatformThreadPool parent;


    PlatformWorkerGroup(int minNumWorkers, int maxNumWorkers,
                        long idleTimeoutNanos,
                        BlockingQueue<Runnable> taskQueue,
                        TaskSubmissionHandler submissionHandler,
                        TaskExceptionHandler exceptionHandler) {

        this.minNumWorkers = minNumWorkers;
        this.maxNumWorkers = maxNumWorkers;
        this.idleTimeoutNanos = idleTimeoutNanos;
        this.taskQueue = taskQueue;
        this.submissionHandler = submissionHandler;
        this.exceptionHandler = exceptionHandler;
    }

    private PlatformThreadPool parent() {
        final PlatformThreadPool parent = this.parent;
        assert parent != null: "parent is not set";
        return parent;
    }

    void setParent(PlatformThreadPool parent) {
        assert this.parent == null : "parent is set already: " + this.parent;
        this.parent = parent;
    }

    int numWorkers() {
        return numWorkers.get();
    }

    int minNumWorkers() {
        return minNumWorkers;
    }

    int maxNumWorkers() {
        return maxNumWorkers;
    }

    int numBusyWorkers() {
        return numBusyWorkers.get();
    }

    void execute(Runnable task) {
        if (!handleLateSubmission(parent(), submissionHandler, task)) {
            return;
        }

        if (!handleSubmission(task)) {
            return;
        }

        addWorkersIfNeeded();
        revertSubmissionIfNeeded(task);
    }

    private boolean handleSubmission(Runnable task) {
        final PlatformThreadPool parent = parent();
        final TaskAction action = submissionHandler.handleSubmission(task, parent);
        if (action == TaskAction.accept()) {
            final Runnable maybeWrappedTask;
            if (parent.metrics() == null) {
                // No need to wrap because metrics were disabled.
                maybeWrappedTask = task;
            } else {
                final Class<?> taskType = task.getClass();
                if (taskType == PlatformThreadFutureTask.class ||
                    taskType == PlatformThreadScheduledFutureTask.class) {
                    // Wrapped already by methods like `submit()` or `schedule()`.
                    maybeWrappedTask = task;
                } else {
                    // Wrap the task so we can measure metrics.
                    maybeWrappedTask = parent.newTaskFor(task, null);
                }
            }
            taskQueue.add(maybeWrappedTask);
            return true;
        }

        action.doAction(task);
        return false;
    }

    private <V extends Runnable> void revertSubmissionIfNeeded(V task) {
        final PlatformThreadPool parent = parent();
        if (parent.isShutdown()) {
            taskQueue.remove(task);
            final boolean accepted = handleLateSubmission(parent, submissionHandler, task);
            assert !accepted : "handleLateSubmission() must return false after shutdown.";
        }
    }

    private void addWorkersIfNeeded() {
        final PlatformThreadPool parent = parent();
        if (needsMoreWorker() != null) {
            workersLock.lock();
            List<Worker> newWorkers = null;
            try {
                // Note that we check if the pool is shut down only *while* acquiring the lock,
                // because:
                // - shutting down a pool doesn't occur very often; and
                // - it's not worth checking whether the pool is shut down or not frequently.
                while (!parent.isShutdown()) {
                    final ExpirationMode expirationMode = needsMoreWorker();
                    if (expirationMode != null) {
                        if (newWorkers == null) {
                            newWorkers = new ArrayList<>();
                        }
                        newWorkers.add(newWorker(expirationMode));
                    } else {
                        break;
                    }
                }
            } finally {
                workersLock.unlock();
            }

            // Call `Thread.start()` call out of the lock window to minimize the contention.
            if (newWorkers != null) {
                final Watchdog watchdog = parent.watchdog();
                if (watchdog != null) {
                    watchdog.start();
                }
                newWorkers.forEach(Worker::start);
            }
        }
    }

    /**
     * Returns the {@link ExpirationMode} of the worker if more worker is needed to handle a newly submitted
     * task. {@code null} is returned if no new worker is needed.
     */
    @Nullable
    private ExpirationMode needsMoreWorker() {
        final int numBusyWorkers = this.numBusyWorkers.get();
        final int numWorkers = this.numWorkers.get();

        if (numWorkers < minNumWorkers) {
            return ExpirationMode.NEVER;
        }

        // Needs more workers if all existing workers are busy.
        if (numBusyWorkers >= numWorkers) {
            // But we shouldn't create more workers than `maxNumWorkers`.
            if (numBusyWorkers < maxNumWorkers) {
                return idleTimeoutNanos > 0 ? ExpirationMode.ON_IDLE : ExpirationMode.NEVER;
            }
        }

        return null;
    }

    private Worker newWorker(ExpirationMode expirationMode) {
        numWorkers.incrementAndGet();
        numBusyWorkers.incrementAndGet();
        final Worker worker = new Worker(expirationMode);
        workers.add(worker);
        return worker;
    }

    boolean hasWorkers() {
        workersLock.lock();
        try {
            return !workers.isEmpty();
        } finally {
            workersLock.unlock();
        }
    }
    void forEachWorker(Consumer<ManagedThread<?>> consumer) {
        for (Worker w : workerArray()) {
            consumer.accept(w);
        }
    }

    Worker[] workerArray() {
        final Worker[] workers;
        workersLock.lock();
        try {
            workers = this.workers.toArray(EMPTY_WORKERS_ARRAY);
        } finally {
            workersLock.unlock();
        }
        return workers;
    }

    int taskQueueSize() {
        return taskQueue.size();
    }

    void drainTaskQueue(List<Runnable> sink) {
        for (;;) {
            final Runnable task = taskQueue.poll();
            if (task == null) {
                break;
            }
            if (task != SHUTDOWN_TASK) {
                sink.add(task);
            }
        }

        // Work around the issues in some queue implementations where elements are not fully drained.
        if (!taskQueue.isEmpty()) {
            for (Runnable task : taskQueue.toArray(EMPTY_RUNNABLES)) {
                if (task != SHUTDOWN_TASK && !sink.contains(task)) {
                    sink.add(task);
                }
            }
        }
    }

    /**
     * Initiates the termination of all worker threads in the group by adding the poison pills
     * to the task queue.
     *
     * @return whether the poison pills were added or not.
     */
    boolean terminateWorkers() {
        final boolean hasWorkers = hasWorkers();
        if (hasWorkers) {
            // Submit the poison pills that will terminate all workers.
            for (int i = 0; i < maxNumWorkers; i++) {
                taskQueue.add(SHUTDOWN_TASK);
            }
        }
        return hasWorkers;
    }

    private enum ExpirationMode {
        /**
         * The worker that never gets terminated.
         * It's only terminated by {@link #SHUTDOWN_TASK}, which is submitted when the pool is shut down.
         */
        NEVER,
        /**
         * The worker that can be terminated due to idle timeout.
         */
        ON_IDLE
    }

    final class Worker extends ManagedThread<Void> {

        private final ExpirationMode expirationMode;

        Worker(ExpirationMode expirationMode) {
            super(PlatformThreadPool.threadFactory);
            this.expirationMode = expirationMode;
        }

        @Override
        @Nullable
        Void call() {
            final PlatformThreadPool parent = parent();
            final ThreadPoolMetrics metrics = parent.metrics();

            logger.debug("Started a new worker: {} (expiration mode: {})", threadName(), expirationMode);

            boolean isBusy = true;
            long lastRunTimeNanos = System.nanoTime();
            try {
                loop:
                for (;;) {
                    Runnable task = null;
                    try {
                        Thread.interrupted(); // Clear the interrupt state.
                        if (parent.state() == ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS) {
                            logger.debug("Terminating a worker by an interrupt: {}", threadName());
                            break;
                        }

                        task = taskQueue.poll();
                        if (task == null) {
                            if (isBusy) {
                                isBusy = false;
                                numBusyWorkers.decrementAndGet();
                            }

                            switch (expirationMode) {
                                case NEVER:
                                    // A core worker is never terminated by idle timeout,
                                    // so we just wait forever.
                                    task = taskQueue.take();
                                    break;
                                case ON_IDLE:
                                    final long waitTimeNanos =
                                            idleTimeoutNanos - (System.nanoTime() - lastRunTimeNanos);
                                    if (waitTimeNanos <= 0 || (task = taskQueue.poll(waitTimeNanos,
                                                                                     TimeUnit.NANOSECONDS)) == null) {
                                        // The worker didn't handle any tasks for a while.
                                        logger.debug("Terminating an idle worker: {}", threadName());
                                        break loop;
                                    }
                                    break;
                                default:
                                    throw new Error();
                            }
                            isBusy = true;
                            numBusyWorkers.incrementAndGet();
                        } else {
                            if (!isBusy) {
                                isBusy = true;
                                numBusyWorkers.incrementAndGet();
                            }
                        }

                        if (task == SHUTDOWN_TASK) {
                            logger.debug("Terminating a worker with a poison pill: {}", threadName());
                            break;
                        } else {
                            boolean success = false;
                            final long taskStartTimeNanos = updateLastActivityTimeNanos();
                            try {
                                task.run();
                                success = true;
                            } finally {
                                clearLastActivityTimeNanos();
                                lastRunTimeNanos = System.nanoTime();
                                if (metrics != null) {
                                    final long taskDurationNanos = lastRunTimeNanos - taskStartTimeNanos;
                                    if (success) {
                                        metrics.successfulTaskExecutionTimer()
                                               .record(taskDurationNanos, TimeUnit.NANOSECONDS);
                                    } else {
                                        metrics.failedTaskExecutionTimer()
                                               .record(taskDurationNanos, TimeUnit.NANOSECONDS);
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        if (parent.state() == ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS) {
                            logger.debug("Terminating a worker by an interrupt: {}", threadName());
                            break;
                        } else if (logger.isTraceEnabled()) {
                            logger.trace("Ignoring an interrupt that's not triggered by shutdownNow()", e);
                        } else {
                            logger.debug("Ignoring an interrupt that's not triggered by shutdownNow()");
                        }
                    } catch (Throwable cause) {
                        invokeExceptionHandler(parent, exceptionHandler, task, cause, logger);
                    }
                }
            } finally {
                workersLock.lock();
                try {
                    workers.remove(this);
                    numWorkers.decrementAndGet();
                    if (isBusy) {
                        numBusyWorkers.decrementAndGet();
                    }

                    if (workers.isEmpty() &&
                        !taskQueue.isEmpty() &&
                        parent.state() != ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS) {
                        for (Runnable task : taskQueue) {
                            if (task != SHUTDOWN_TASK) {
                                // We found the situation where:
                                // - there are no active workers available; and
                                // - there is a task in the queue.
                                // Start a new worker so that it's picked up.
                                addWorkersIfNeeded();
                                break;
                            }
                        }
                    }
                } finally {
                    workersLock.unlock();
                }

                logger.debug("A worker has been terminated: {} (expiration mode: {})",
                             threadName(), expirationMode);
            }

            return null;
        }
    }
}
