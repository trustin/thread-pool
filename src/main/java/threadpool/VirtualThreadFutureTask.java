package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

final class VirtualThreadFutureTask<V> extends ManagedThread<V> {

    private final Callable<V> task;
    private final VirtualThreadPool parent;

    VirtualThreadFutureTask(VirtualThreadPool parent, Runnable task, @Nullable V result) {
        this(parent, Executors.callable(task, result));
    }

    VirtualThreadFutureTask(VirtualThreadPool parent, Callable<V> task) {
        super(VirtualThreadPool.threadFactory);
        this.task = task;
        this.parent = parent;
    }

    @Override
    public boolean isDone() {
        final boolean done = super.isDone();
        if (done) {
            return true;
        }

        if (parent.state() == ThreadPoolState.SHUT_DOWN_WITH_INTERRUPTS) {
            return cancel(false);
        }

        return false;
    }

    @Override
    @Nullable
    V call() throws Exception {
        boolean removed = false;
        try {
            parent.addActiveTask(this);
            final V result = task.call();
            parent.removeActiveTask(this);
            removed = true;
            return result;
        } finally {
            if (!removed) {
                parent.removeActiveTask(this);
            }
        }
    }
}
