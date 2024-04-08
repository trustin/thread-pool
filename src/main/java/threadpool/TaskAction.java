package threadpool;

import java.util.concurrent.Callable;

public interface TaskAction {
    static TaskAction accept() {
        return TaskActions.ACCEPT;
    }
    static TaskAction reject() {
        return TaskActions.REJECT;
    }
    static TaskAction log() {
        return TaskActions.LOG;
    }

    void doAction(Runnable task);
    void doAction(Callable<?> task);
}
