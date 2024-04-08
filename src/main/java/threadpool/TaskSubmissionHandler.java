package threadpool;

import java.util.concurrent.Callable;

public interface TaskSubmissionHandler {

    static TaskSubmissionHandler ofDefault() {
        return DefaultTaskSubmissionHandler.INSTANCE;
    }

    TaskAction handleSubmission(Runnable task, ThreadPool threadPool);
    TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool);

    TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool);
    TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool);
}
