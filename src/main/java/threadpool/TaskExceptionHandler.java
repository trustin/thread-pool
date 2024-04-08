package threadpool;

import java.util.concurrent.Callable;

public interface TaskExceptionHandler {

    static TaskExceptionHandler ofDefault() {
        return DefaultTaskExceptionHandler.INSTANCE;
    }

    void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool);
    void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool);
}
