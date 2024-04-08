package threadpool;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum DefaultTaskExceptionHandler implements TaskExceptionHandler {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(DefaultTaskExceptionHandler.class);

    @Override
    public void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool) {
        log(cause);
    }

    @Override
    public void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool) {
        log(cause);
    }

    private static void log(Throwable cause) {
        logger.warn("Unexpected exception while running a task:", cause);
    }
}
