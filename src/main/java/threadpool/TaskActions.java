package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TaskActions {

    private static final Logger logger = LoggerFactory.getLogger(TaskActions.class);

    static final TaskAction ACCEPT = new TaskAction() {
        @Override
        public void doAction(Runnable task) {}

        @Override
        public void doAction(Callable<?> task){}
    };

    static final TaskAction REJECT = new TaskAction() {
        @Override
        public void doAction(Runnable task) {
            reject();
        }

        @Override
        public void doAction(Callable<?> task) {
            reject();
        }

        private void reject() {
            throw new RejectedExecutionException();
        }
    };


    static final TaskAction LOG = new TaskAction() {
        @Override
        public void doAction(Runnable task) {
            log(task);
        }

        @Override
        public void doAction(Callable<?> task) {
            log(task);
        }

        private void log(Object task) {
            logger.warn("Rejected a task: {}", task);
        }
    };

    private TaskActions() {}
}
