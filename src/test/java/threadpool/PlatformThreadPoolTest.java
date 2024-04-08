package threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PlatformThreadPoolTest {

    private static final Logger logger = LoggerFactory.getLogger(PlatformThreadPoolTest.class);

    @Test
    void submittedTasksAreExecuted() throws Exception {
        final ThreadPool executor = ThreadPool.builder(6)
                                              .minNumWorkers(3)
                                              //.taskTimeout(100, TimeUnit.MILLISECONDS)
                                              .watchdogInterval(10, TimeUnit.MILLISECONDS)
                                              .build();

        final int numTasks = 1000;
        final List<CompletableFuture<String>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < numTasks; i++) {
                final int finalI = i;
                final CompletableFuture<String> f = executor
                        .submit(new Callable<String>() {
                            @Override
                            public String call() {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    logger.info("{} has been interrupted", this);
                                }
                                return String.valueOf(finalI);
                            }

                            @Override
                            public String toString() {
                                return "Task " + finalI;
                            }
                        })
                        .thenApply(value -> "Task " + value)
                        .whenComplete((value, cause) -> logger.info(value));

                futures.add(f);
            }

            futures.forEach(CompletableFuture::join);
        } finally {
            logger.info("====================");
            executor.shutdown();
            logger.info("===============================");
            executor.awaitTermination();
        }
    }

    @Test
    void customTaskSubmissionHandler() throws InterruptedException {
        final Runnable taskToReject = () -> {
        };
        final ThreadPool pool = ThreadPool
                .builder(1)
                .submissionHandler(new TaskSubmissionHandler() {
                    @Override
                    public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
                        return task == taskToReject ? TaskAction.reject() :
                               TaskAction.accept();
                    }

                    @Override
                    public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
                        return TaskAction.reject();
                    }

                    @Override
                    public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
                        return TaskAction.reject();
                    }
                })
                .build();

        // Should accept a task that's not `taskToReject`.
        final CountDownLatch latch = new CountDownLatch(1);
        pool.execute(latch::countDown);
        latch.await();

        //pool.execute(taskToReject);

        pool.shutdown();
        pool.execute(() -> {});
    }
}
