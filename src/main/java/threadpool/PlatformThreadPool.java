package threadpool;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

final class PlatformThreadPool extends AbstractThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(PlatformThreadPool.class);

    static final ThreadFactory threadFactory = Executors.defaultThreadFactory();

    private final PlatformWorkerGroup workers;
    private final PlatformWorkerGroup scheduledWorkers;
    private final Scheduler scheduler;
    @Nullable
    private final Timer taskWaitTimer;
    @Nullable
    private final ThreadPoolMetrics metrics;

    PlatformThreadPool(PlatformWorkerGroup workers, PlatformWorkerGroup scheduledWorkers,
                       long taskTimeoutNanos, long watchdogIntervalNanos,
                       MeterRegistry meterRegistry,
                       @Nullable String metricPrefix,
                       @Nullable List<Tag> metricTags) {

        super(taskTimeoutNanos, watchdogIntervalNanos);

        this.workers = workers;
        this.scheduledWorkers = scheduledWorkers;

        scheduler = new Scheduler();

        // Set up metrics
        if (metricPrefix != null) {
            assert metricTags != null : "metricTags is null";

            final Gauge state =
                    Gauge.builder(metricPrefix + ".state", () -> state().ordinal())
                         .tags(metricTags)
                         .register(meterRegistry);

            final Timer successfulTaskExecutionTimer =
                    Timer.builder(metricPrefix + ".task.execution")
                         .tags(metricTags)
                         .tag("result", "success")
                         .register(meterRegistry);

            final Timer failedTaskExecutionTimer =
                    Timer.builder(metricPrefix + ".task.execution")
                         .tags(metricTags)
                         .tag("result", "failure")
                         .register(meterRegistry);

            final Gauge scheduledTaskCount =
                    Gauge.builder(metricPrefix + ".scheduled.tasks", () -> scheduler.numScheduledTasks)
                         .tags(metricTags)
                         .register(meterRegistry);

            final Gauge workerCount;
            final Gauge scheduledWorkerCount;
            final Gauge busyWorkerCount;
            final Gauge busyScheduledWorkerCount;
            final Gauge minNumWorkerCount;
            final Gauge minNumScheduledWorkerCount;
            final Gauge maxNumWorkerCount;
            final Gauge maxNumScheduledWorkerCount;
            final Gauge taskQueueSize;
            final Gauge scheduledTaskQueueSize;

            if (scheduledWorkers == null) {
                workerCount =
                        Gauge.builder(metricPrefix + ".workers", workers::numWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "maybe")
                             .register(meterRegistry);

                busyWorkerCount =
                        Gauge.builder(metricPrefix + ".busy.workers", workers::numBusyWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "maybe")
                             .register(meterRegistry);

                minNumWorkerCount =
                        Gauge.builder(metricPrefix + ".min.workers", workers::minNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "maybe")
                             .register(meterRegistry);

                maxNumWorkerCount =
                        Gauge.builder(metricPrefix + ".max.workers", workers::maxNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "maybe")
                             .register(meterRegistry);

                taskQueueSize =
                        Gauge.builder(metricPrefix + ".task.queue.size", workers::taskQueueSize)
                             .tags(metricTags)
                             .tag("scheduled", "maybe")
                             .register(meterRegistry);

                scheduledWorkerCount = null;
                busyScheduledWorkerCount = null;
                minNumScheduledWorkerCount = null;
                maxNumScheduledWorkerCount = null;
                scheduledTaskQueueSize = null;
            } else {
                workerCount =
                        Gauge.builder(metricPrefix + ".workers", workers::numWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "false")
                             .register(meterRegistry);

                scheduledWorkerCount =
                        Gauge.builder(metricPrefix + ".workers", scheduledWorkers::numWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "true")
                             .register(meterRegistry);

                busyWorkerCount =
                        Gauge.builder(metricPrefix + ".busy.workers", workers::numBusyWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "false")
                             .register(meterRegistry);

                busyScheduledWorkerCount =
                        Gauge.builder(metricPrefix + ".busy.workers", workers::numBusyWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "true")
                             .register(meterRegistry);

                minNumWorkerCount =
                        Gauge.builder(metricPrefix + ".min.workers", workers::minNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "false")
                             .register(meterRegistry);

                minNumScheduledWorkerCount =
                        Gauge.builder(metricPrefix + ".min.workers", scheduledWorkers::minNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "true")
                             .register(meterRegistry);

                maxNumWorkerCount =
                        Gauge.builder(metricPrefix + ".max.workers", workers::maxNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "false")
                             .register(meterRegistry);

                maxNumScheduledWorkerCount =
                        Gauge.builder(metricPrefix + ".max.workers", scheduledWorkers::maxNumWorkers)
                             .tags(metricTags)
                             .tag("scheduled", "true")
                             .register(meterRegistry);

                taskQueueSize =
                        Gauge.builder(metricPrefix + ".task.queue.size", workers::taskQueueSize)
                             .tags(metricTags)
                             .tag("scheduled", "false")
                             .register(meterRegistry);

                scheduledTaskQueueSize =
                        Gauge.builder(metricPrefix + ".task.queue.size", scheduledWorkers::taskQueueSize)
                             .tags(metricTags)
                             .tag("scheduled", "true")
                             .register(meterRegistry);
            }

            taskWaitTimer =
                    Timer.builder(metricPrefix + ".task.wait")
                         .tags(metricTags)
                         .register(meterRegistry);

            metrics = new ThreadPoolMetrics(
                    state,
                    workerCount,
                    scheduledWorkerCount,
                    busyWorkerCount,
                    busyScheduledWorkerCount,
                    minNumWorkerCount,
                    minNumScheduledWorkerCount,
                    maxNumWorkerCount,
                    maxNumScheduledWorkerCount,
                    taskQueueSize,
                    scheduledTaskQueueSize,
                    successfulTaskExecutionTimer,
                    failedTaskExecutionTimer,
                    taskWaitTimer,
                    scheduledTaskCount
            );
        } else {
            taskWaitTimer = null;
            metrics = null;
        }
    }

    @Override
    public boolean isVirtual() {
        return false;
    }

    @Nullable
    @Override
    public ThreadPoolMetrics metrics() {
        return metrics;
    }

    @Override
    List<Runnable> drainUnprocessedTasks() {
        final int taskQueueSize;
        if (workers == scheduledWorkers) {
            taskQueueSize = workers.taskQueueSize();
        } else {
            taskQueueSize = workers.taskQueueSize() + scheduledWorkers.taskQueueSize();
        }

        final List<Runnable> unprocessed = new ArrayList<>(taskQueueSize);
        workers.drainTaskQueue(unprocessed);
        scheduledWorkers.drainTaskQueue(unprocessed);
        return unprocessed;
    }

    @Override
    boolean hasWorkers() {
        return workers.hasWorkers() || scheduledWorkers.hasWorkers();
    }

    @Override
    void forEachWorker(Consumer<ManagedThread<?>> block) {
        workers.forEachWorker(block);
        if (workers != scheduledWorkers) {
            scheduledWorkers.forEachWorker(block);
        }
    }

    @Override
    boolean terminateWorkers() {
        boolean hasWorkers = workers.terminateWorkers();
        if (workers != scheduledWorkers) {
            hasWorkers |= scheduledWorkers.terminateWorkers();
        }
        return hasWorkers;
    }

    @Override
    public void execute(Runnable command) {
        workers.execute(command);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, @Nullable T value) {
        return new PlatformThreadFutureTask<>(runnable, value, taskWaitTimer);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new PlatformThreadFutureTask<>(callable, taskWaitTimer);
    }

    @Override
    public CompletableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        requireNonNull(command, "command");
        final long delayNanos = validateDelay(delay, unit, "delay");
        return scheduler.schedule(new PlatformThreadScheduledFutureTask<>(scheduler, command, null, delayNanos, 0,
                                                                       metrics != null ? metrics.taskWaitTimer() : null));
    }

    @Override
    public <V> CompletableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        requireNonNull(callable, "callable");
        final long delayNanos = validateDelay(delay, unit, "delay");
        return scheduler.schedule(new PlatformThreadScheduledFutureTask<>(scheduler, callable, delayNanos,
                                                                          taskWaitTimer));
    }

    @Override
    public CompletableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                             TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long periodNanos = validateDelay(period, unit, "period");
        final PlatformThreadScheduledFutureTask<Object> task = new PlatformThreadScheduledFutureTask<>(scheduler, command, null,
                                                                                                       initialDelayNanos, periodNanos,
                                                                                                       taskWaitTimer);
        return scheduler.schedule(task);
    }

    @Override
    public CompletableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                                TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final PlatformThreadScheduledFutureTask<Object> task = new PlatformThreadScheduledFutureTask<>(scheduler, command, null,
                                                                                                       initialDelayNanos, -delayNanos,
                                                                                                       taskWaitTimer);
        return scheduler.schedule(task);
    }

    static long validateDelay(long delay, TimeUnit unit, String name) {
        requireNonNull(unit, "unit");

        final long delayNanos = unit.toNanos(delay);
        if (delayNanos < 0 || delayNanos >= (Long.MAX_VALUE / 2)) {
            // e.g. "initialDelay: -42 DAYS (expected: 0 <= initialDelay < 146 years)"
            throw new IllegalArgumentException(
                    name + ": " + delay + ' ' + unit +
                    " (expected: 0 <= " + name + " < 146 years)");
        }

        return delayNanos;
    }

    final class Scheduler extends ManagedThread<Void> {

        final LongAdder numScheduledTasks;
        private final Lock lock;
        private final Condition condition;
        private PlatformThreadScheduledFutureTask<?>[] queue;
        private int queueSize;

        Scheduler() {
            super(threadFactory);
            numScheduledTasks = new LongAdder();
            lock = new ReentrantLock();
            condition = lock.newCondition();
            queue = new PlatformThreadScheduledFutureTask[1024];
        }

        <V> PlatformThreadScheduledFutureTask<V> schedule(PlatformThreadScheduledFutureTask<V> task) {
            acceptScheduledTask(task);
            numScheduledTasks.increment();
            start();
            return task;
        }

        void reschedule(PlatformThreadScheduledFutureTask<?> task) {
            if (isShutdown()) {
                task.cancel(false);
            }

            acceptScheduledTask(task);

            if (isShutdown()) {
                final boolean removed = removeScheduledTask(task);
                if (removed) {
                    task.cancel(false);
                }
            }
        }

        private void acceptScheduledTask(PlatformThreadScheduledFutureTask<?> task) {
            task.ensureUnscheduled();

            lock.lock();
            try {
                // Check that the array capacity is enough to hold values by doubling capacity.
                if (queueSize >= queue.length) {
                    // Use a policy which allows for a 0 initial capacity. Same policy as JDK's priority queue, double when
                    // "small", then grow by 50% when "large".
                    queue = Arrays.copyOf(queue, queue.length + ((queue.length < 64) ?
                                                                 (queue.length + 2) :
                                                                 (queue.length >>> 1)));
                }

                bubbleUp(queueSize++, task);

                // Notify the scheduler loop if the earliest task has changed.
                if (task.schedulerIndex() == 0) {
                    logger.info("signal()");
                    condition.signal();
                } else {
                    logger.info("signal() not invoked");
                }
            } finally {
                lock.unlock();
            }
        }

        private PlatformThreadScheduledFutureTask<?> takeScheduledTask() throws InterruptedException {
            lock.lock();
            try {
                PlatformThreadScheduledFutureTask<?> first;
                for (;;) {
                    first = queue[0];
                    if (first == null) {
                        logger.info("await() {}", System.currentTimeMillis());
                        condition.await();
                        continue;
                    }

                    final long delayNanos = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delayNanos > 0) {
                        // Need to wait until the task is ready to run.
                        logger.info("awaitNanos({}) {}", delayNanos, System.currentTimeMillis());
                        final long nextDelayNanos = condition.awaitNanos(delayNanos);
                        logger.info("= {} {}", nextDelayNanos, System.currentTimeMillis());
                        if (nextDelayNanos > 0) {
                            // Need to wait a little bit more.
                            continue;
                        }
                    }
                    break;
                }

                first.setUnscheduled();

                final PlatformThreadScheduledFutureTask<?> last = queue[--queueSize];
                queue[queueSize] = null;
                if (queueSize != 0) { // Make sure we don't add the last element back.
                    bubbleDown(0, last);
                }

                return first;
            } finally {
                lock.unlock();
            }
        }

        boolean removeScheduledTask(PlatformThreadScheduledFutureTask<?> task) {
            lock.lock();
            try {
                final int i = task.schedulerIndex();
                if (!contains(task, i)) {
                    return false;
                }

                task.setUnscheduled();
                if (--queueSize == 0 || queueSize == i) {
                    // If there are no node left, or this is the last node in the array just remove and return.
                    queue[i] = null;
                    return true;
                }

                // Move the last element where node currently lives in the array.
                final PlatformThreadScheduledFutureTask<?> moved = queue[i] = queue[queueSize];
                queue[queueSize] = null;
                // priorityQueueIndex will be updated below in bubbleUp or bubbleDown

                // Make sure the moved node still preserves the min-heap properties.
                if (task.compareTo(moved) < 0) {
                    bubbleDown(i, moved);
                } else {
                    bubbleUp(i, moved);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        private boolean contains(PlatformThreadScheduledFutureTask<?> node, int i) {
            return i >= 0 && i < queueSize && node.equals(queue[i]);
        }

        private void bubbleDown(int k, PlatformThreadScheduledFutureTask<?> node) {
            final int half = queueSize >>> 1;
            while (k < half) {
                // Compare node to the children of index k.
                int iChild = (k << 1) + 1;
                PlatformThreadScheduledFutureTask<?> child = queue[iChild];

                // Make sure we get the smallest child to compare against.
                final int rightChild = iChild + 1;
                if (rightChild < queueSize && child.compareTo(queue[rightChild]) > 0) {
                    child = queue[iChild = rightChild];
                }
                // If the bubbleDown node is less than or equal to the smallest child then we will preserve the min-heap
                // property by inserting the bubbleDown node here.
                if (node.compareTo(child) <= 0) {
                    break;
                }

                // Bubble the child up.
                queue[k] = child;
                child.setScheduled(k);

                // Move down k down the tree for the next iteration.
                k = iChild;
            }

            // We have found where node should live and still satisfy the min-heap property, so put it in the queue.
            queue[k] = node;
            node.setScheduled(k);
        }

        private void bubbleUp(int k, PlatformThreadScheduledFutureTask<?> node) {
            while (k > 0) {
                final int iParent = (k - 1) >>> 1;
                final PlatformThreadScheduledFutureTask<?> parent = queue[iParent];

                // If the bubbleUp node is less than the parent, then we have found a spot to insert and still maintain
                // min-heap properties.
                if (node.compareTo(parent) >= 0) {
                    break;
                }

                // Bubble the parent down.
                queue[k] = parent;
                parent.setScheduled(k);

                // Move k up the tree for the next iteration.
                k = iParent;
            }

            // We have found where node should live and still satisfy the min-heap property, so put it in the queue.
            queue[k] = node;
            node.setScheduled(k);
        }

        @Override
        @Nullable
        Void call() {
            try {
                for (;;) {
                    final PlatformThreadScheduledFutureTask<?> task;
                    try {
                        task = takeScheduledTask();
                    } catch (InterruptedException e) {
                        if (isShutdown()) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    logger.info("addScheduledTask() {}", System.currentTimeMillis());
                    if (!task.isDone()) {
                        task.markWaitTimeStart();
                        scheduledWorkers.execute(task);
                    }
                    logger.info("end of addScheduledTask() {}", System.currentTimeMillis());
                }
            } finally {
                final PlatformThreadScheduledFutureTask<?>[] clone;
                lock.lock();
                try {
                    clone = queue.clone();
                    Arrays.fill(queue, null);
                } finally {
                    lock.unlock();
                }

                for (PlatformThreadScheduledFutureTask<?> task : clone) {
                    if (task == null) {
                        // Reached at the end of the queue.
                        break;
                    }
                    task.setUnscheduled();
                    task.cancel(false);
                }
            }

            return null;
        }
    }
}
