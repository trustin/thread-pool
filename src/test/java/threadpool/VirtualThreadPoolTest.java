package threadpool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class VirtualThreadPoolTest {

    private final ThreadPool threadPool = ThreadPool.ofVirtual();

    @Test
    void testExecute() throws Exception {
        final int numTasks = 1000;
        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            final int finalI = i;
            futures.add(threadPool.submit(() -> System.err.println(finalI)));
        }

        futures.forEach(CompletableFuture::join);
    }

    @Test
    void testSchedule() throws Exception {
        System.err.println(new Date());
        final CompletableScheduledFuture<?> f = threadPool.schedule(() -> {
            System.err.println(new Date());
        }, 3, TimeUnit.SECONDS);

        f.join();
        System.err.println("The scheduled task was run.");
    }

    @Test
    void testScheduleWithFixedDelay() throws Exception {
        System.err.println(new Date());
        final CompletableScheduledFuture<?> f = threadPool.scheduleWithFixedDelay(() -> {
            System.err.println(new Date());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 3, 3, TimeUnit.SECONDS);

        f.join();
        System.err.println("The scheduled task was run.");
    }

    @Test
    void testShutdown() throws InterruptedException {
        final ThreadPool threadPool = ThreadPool.ofVirtual();
        threadPool.schedule(() -> {}, 1, TimeUnit.SECONDS);
//        threadPool.scheduleWithFixedDelay(() -> {}, 1, 1, TimeUnit.SECONDS);
//        threadPool.execute(() -> {
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });

        Thread.sleep(2000);
        System.err.println(threadPool.shutdownNow());
        System.err.println("shutdownNow() returned");
        threadPool.awaitTermination();
    }
}
