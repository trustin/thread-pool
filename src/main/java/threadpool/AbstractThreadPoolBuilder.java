package threadpool;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

abstract class AbstractThreadPoolBuilder<SELF extends AbstractThreadPoolBuilder<SELF>> {

    private static final long DEFAULT_WATCHDOG_INTERVAL_SECONDS = 1;

    private TaskSubmissionHandler submissionHandler = TaskSubmissionHandler.ofDefault();
    private TaskExceptionHandler exceptionHandler = TaskExceptionHandler.ofDefault();
    private long taskTimeoutNanos; // 0 means 'disabled'.
    private long watchdogIntervalNanos = TimeUnit.SECONDS.toNanos(DEFAULT_WATCHDOG_INTERVAL_SECONDS);
    private MeterRegistry meterRegistry = Metrics.globalRegistry;
    @Nullable
    private String metricPrefix;
    private List<Tag> metricTags = ImmutableList.of();

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }

    public final SELF taskTimeout(long taskTimeout, TimeUnit unit) {
        checkArgument(taskTimeout >= 0, "taskTimeout: %s (expected: >= 0)");
        taskTimeoutNanos = requireNonNull(unit, "unit").toNanos(taskTimeout);
        return self();
    }

    public final SELF taskTimeout(Duration taskTimeout) {
        requireNonNull(taskTimeout, "taskTimeout");
        checkArgument(!taskTimeout.isNegative(), "taskTimeout: %s (expected: >= 0)");
        return taskTimeout(taskTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    public final SELF watchdogInterval(long watchdogInterval, TimeUnit unit) {
        checkArgument(watchdogInterval > 0, "watchdogInterval: %s (expected: > 0)");
        watchdogIntervalNanos = requireNonNull(unit, "unit").toNanos(watchdogInterval);
        return self();
    }

    public final SELF watchdogInterval(Duration watchdogInterval) {
        requireNonNull(watchdogInterval, "watchdogInterval");
        checkArgument(!watchdogInterval.isZero() &&
                      !watchdogInterval.isNegative(),
                      "watchdogInterval: %s (expected: > 0)");
        return watchdogInterval(watchdogInterval.toNanos(), TimeUnit.NANOSECONDS);
    }


    public final SELF submissionHandler(TaskSubmissionHandler submissionHandler) {
        this.submissionHandler = requireNonNull(submissionHandler, "submissionHandler");
        return self();
    }

    public final SELF exceptionHandler(TaskExceptionHandler exceptionHandler) {
        this.exceptionHandler = requireNonNull(exceptionHandler, "exceptionHandler");
        return self();
    }


    public final SELF metrics(String metricPrefix, Iterable<? extends Tag> metricTags) {
        return metrics(Metrics.globalRegistry, metricPrefix, metricTags);
    }

    public final SELF metrics(MeterRegistry meterRegistry, String metricPrefix,
                                     Tag... metricTags) {
        return metrics(meterRegistry, metricPrefix,
                       ImmutableList.copyOf(requireNonNull(metricTags, "metricTags")));
    }

    public final SELF metrics(MeterRegistry meterRegistry, String metricPrefix,
                                     Iterable<? extends Tag> metricTags) {
        requireNonNull(meterRegistry, "meterRegistry");
        requireNonNull(metricPrefix, "metricPrefix");
        requireNonNull(metricTags, "metricTags");
        this.meterRegistry = meterRegistry;
        this.metricPrefix = metricPrefix;
        this.metricTags = ImmutableList.copyOf(metricTags);
        return self();
    }

    public final ThreadPool build() {
        return build(submissionHandler, exceptionHandler,
                     taskTimeoutNanos, watchdogIntervalNanos,
                     meterRegistry, metricPrefix, metricTags);
    }

    abstract ThreadPool build(TaskSubmissionHandler submissionHandler, TaskExceptionHandler exceptionHandler,
                              long taskTimeoutNanos, long watchdogIntervalNanos,
                              MeterRegistry meterRegistry, @Nullable String metricPrefix, List<Tag> metricTags);
}
