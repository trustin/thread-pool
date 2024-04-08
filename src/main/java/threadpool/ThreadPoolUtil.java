package threadpool;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import javax.annotation.Nullable;

import org.slf4j.Logger;

final class ThreadPoolUtil {

    static boolean handleLateSubmission(ThreadPool threadPool, TaskSubmissionHandler submissionHandler, Runnable task) {
        if (!threadPool.isShutdown()) {
            return true;
        }

        final TaskAction action = submissionHandler.handleLateSubmission(task, threadPool);
        checkState(action != TaskAction.accept(),
                   "TaskSubmissionHandler.handleLateSubmission() should never accept a task.");
        action.doAction(task);
        return false;
    }

    static void invokeExceptionHandler(ThreadPool threadPool, TaskExceptionHandler exceptionHandler,
                                       @Nullable Runnable task, Throwable cause, Logger logger) {

        if (task == null) {
            logger.warn("Unexpected exception from a thread pool:", cause);
            return;
        }

        try {
            exceptionHandler.handleTaskException(task, cause, threadPool);
        } catch (Throwable handlerCause) {
            handlerCause.addSuppressed(cause);
            logger.warn("Unexpected exception from a task exception handler:", handlerCause);
        }
    }

    static <T> int compareScheduledTasks(T a,
                                         long deadlineNanosA,
                                         T b,
                                         long deadlineNanosB,
                                         AtomicIntegerFieldUpdater<T> differentiatorUpdater,
                                         AtomicInteger differentiatorGenerator) {

        final long diff = deadlineNanosA - deadlineNanosB;
        if (diff < 0) {
            return -1;
        }
        if (diff > 0) {
            return 1;
        }

        final int thisId = System.identityHashCode(a);
        final int thatId = System.identityHashCode(b);
        if (thisId < thatId) {
            return -1;
        }
        if (thisId > thatId) {
            return 1;
        }

        final int thisDifferentiator = differentiator(a, differentiatorUpdater, differentiatorGenerator);
        final int thatDifferentiator = differentiator(b, differentiatorUpdater, differentiatorGenerator);
        if (thisDifferentiator < thatDifferentiator) {
            return -1;
        }
        if (thisDifferentiator > thatDifferentiator) {
            return 1;
        }

        throw new AssertionError("Can't return 0 for two different objects");
    }

    private static <T> int differentiator(T task,
                                          AtomicIntegerFieldUpdater<T> updater,
                                          AtomicInteger generator) {
        for (;;) {
            final int oldDifferentiator = updater.get(task);
            if (oldDifferentiator != 0) {
                return oldDifferentiator;
            }

            final int newDifferentiator = generator.incrementAndGet();
            if (newDifferentiator == 0) {
                continue;
            }

            if (!updater.compareAndSet(task, 0, newDifferentiator)) {
                continue;
            }

            return newDifferentiator;
        }
    }

    private ThreadPoolUtil() {}
}
