package threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface ThreadPool extends ScheduledExecutorService {

    static ThreadPool of(int maxNumWorkers) {
        return builder(maxNumWorkers).build();
    }

    static ThreadPool of(int minNumWorkers, int maxNumWorkers) {
        return builder(maxNumWorkers).minNumWorkers(minNumWorkers).build();
    }

    static ThreadPoolBuilder builder(int maxNumWorkers) {
        return new ThreadPoolBuilder(maxNumWorkers);
    }

    static ThreadPool ofVirtual() {
        return builderForVirtual().build();
    }

    static VirtualThreadPoolBuilder builderForVirtual() {
        return new VirtualThreadPoolBuilder();
    }

    boolean isVirtual();

    @Override
    <T> CompletableFuture<T> submit(Callable<T> task);

    @Override
    <T> CompletableFuture<T> submit(Runnable task, T result);

    @Override
    CompletableFuture<?> submit(Runnable task);

    @Override
    CompletableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    @Override
    <V> CompletableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

    @Override
    CompletableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    @Override
    CompletableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);

    ThreadPoolMetrics metrics();

    void awaitTermination() throws InterruptedException;

    CompletableFuture<?> terminationFuture();
}
