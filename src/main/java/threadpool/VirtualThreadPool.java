package threadpool;

import static java.util.Objects.requireNonNull;
import static threadpool.PlatformThreadPool.validateDelay;
import static threadpool.ThreadPoolUtil.handleLateSubmission;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

final class VirtualThreadPool extends AbstractThreadPool {

    static final ThreadFactory threadFactory = Thread.ofVirtual().factory();

    // TODO: Virtual threads compatibility
    //       - Implement shutdown/shutdownNow() in a way that can handle millions of virtual threads,
    //         unlike JDK's ThreadPerTaskExecutor.
    //         - Only keep the set of active virtual threads.
    //         - Wrap all tasks so that it exits itself when the pool has been shut down.
    //       - Implement metrics
    // TODO: Write real test cases.
    // TODO: Write JMH benchmarks and compare against JDK ThreadPoolExecutor.

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadPool.class);

    private final TaskSubmissionHandler submissionHandler;
    private final TaskExceptionHandler exceptionHandler;

    private final Set<ManagedThread<?>> activeTasks = ConcurrentHashMap.newKeySet();

    VirtualThreadPool(TaskSubmissionHandler submissionHandler,
                      TaskExceptionHandler exceptionHandler,
                      long taskTimeoutNanos, long watchdogIntervalNanos,
                      MeterRegistry meterRegistry, @Nullable String metricPrefix, List<Tag> metricTags) {
        super(taskTimeoutNanos, watchdogIntervalNanos);
        this.submissionHandler = submissionHandler;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public boolean isVirtual() {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        requireNonNull(command, "command");
        if (!handleLateSubmission(this, submissionHandler, command)) {
            return;
        }

        handleSubmission(command);
    }

    private void handleSubmission(Runnable task) {
        final TaskAction action = submissionHandler.handleSubmission(task, this);
        if (action == TaskAction.accept()) {
            final ManagedThread<?> wrappedTask;
            final Class<?> taskType = task.getClass();
            if (taskType == VirtualThreadFutureTask.class ||
                taskType == VirtualThreadScheduledFutureTask.class) {
                // Wrapped already by methods like `submit()` or `schedule()`.
                wrappedTask = (ManagedThread<?>) task;
            } else {
                // Wrap the task so we can measure metrics.
                wrappedTask = newTaskFor(task, null);
            }

            wrappedTask.start();
        } else {
            action.doAction(task);
        }
    }

    @Override
    protected <T> ManagedThread<T> newTaskFor(Runnable runnable, @Nullable T value) {
        return new VirtualThreadFutureTask<>(this, runnable, value);
    }

    @Override
    protected <T> ManagedThread<T> newTaskFor(Callable<T> callable) {
        return new VirtualThreadFutureTask<>(this, callable);
    }

    @Override
    public CompletableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        requireNonNull(command, "command");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<?> task =
                new VirtualThreadScheduledFutureTask<>(command, null, delayNanos, 0);
        execute(task);
        return task;
    }

    @Override
    public <V> CompletableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        requireNonNull(callable, "callable");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<V> task =
                new VirtualThreadScheduledFutureTask<>(callable, delayNanos);
        execute(task);
        return task;
    }

    @Override
    public CompletableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                             TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long periodNanos = validateDelay(period, unit, "period");
        final VirtualThreadScheduledFutureTask<Object> task =
                new VirtualThreadScheduledFutureTask<>(command, null, initialDelayNanos, periodNanos);
        execute(task);
        return task;
    }

    @Override
    public CompletableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                                TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<Object> task =
                new VirtualThreadScheduledFutureTask<>(command, null, initialDelayNanos, -delayNanos);
        execute(task);
        return task;
    }

    void addActiveTask(ManagedThread<?> task) {
        activeTasks.add(task);
    }

    void removeActiveTask(ManagedThread<?> task) {
        activeTasks.remove(task);
    }

    @Override
    public ThreadPoolMetrics metrics() {
        return null;
    }

    @Override
    List<Runnable> drainUnprocessedTasks() {
        return List.of();
    }

    @Override
    boolean hasWorkers() {
        return false;
    }

    @Override
    void forEachWorker(Consumer<ManagedThread<?>> block) {

    }

    @Override
    boolean terminateWorkers() {
        return false;
    }
}
