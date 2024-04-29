package threadpool;

import java.util.concurrent.Callable;

import javax.annotation.Nullable;

abstract class ManagedVirtualThread<V> extends ManagedThread<V> {

    private final VirtualThreadPool parent;
    private final Callable<V> task;

    ManagedVirtualThread(VirtualThreadPool parent, Callable<V> task) {
        super(VirtualThreadPool.threadFactory);
        this.parent = parent;
        this.task = task;
    }

    @Override
    final void onStart() {
        parent.addTask(this);
    }

    @Override
    final void onStop() {
        parent.removeTask(this);
    }

    @Nullable
    @Override
    V call() throws Exception {
        activate();
        try {
            return task.call();
        } finally {
            deactivate();
        }
    }

    private void activate() {
        updateLastActivityTimeNanos();
        parent.addActiveTask(this);
    }

    private void deactivate() {
        parent.removeActiveTask(this);
        clearLastActivityTimeNanos();
    }
}
