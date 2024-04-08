package threadpool;

import java.util.concurrent.Callable;

enum DefaultTaskSubmissionHandler implements TaskSubmissionHandler {

    INSTANCE;

    @Override
    public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
        return TaskAction.reject();
    }

    @Override
    public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
        return TaskAction.reject();
    }
}
