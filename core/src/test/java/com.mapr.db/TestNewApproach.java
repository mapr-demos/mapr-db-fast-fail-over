package com.mapr.db;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestNewApproach {

//    @Test
    public void testName() throws Exception {
        ExecutorService taskExecutor = Executors.newFixedThreadPool(2);
        CompletionService taskCompletionService = new ExecutorCompletionService<>(taskExecutor);

        MyCallable task1 = new MyCallable(10001, "First");
        MyCallable task2 = new MyCallable(0, "Second");

        taskCompletionService.submit(task1);

        ScheduledExecutorService executorService =
                Executors.newScheduledThreadPool(1);

        executorService.schedule(() -> {
            System.out.println("Submitting second task...");
            taskCompletionService.submit(task2);
        }, 5, TimeUnit.SECONDS);

        Future take = taskCompletionService.take();
        System.out.println(take.get());
        executorService.shutdownNow();
    }

    class MyCallable implements Callable {
        int timeToWait;
        String result;

        public MyCallable(int timeToWaitMs, String result) {
            this.timeToWait = timeToWaitMs;
            this.result = result;
        }

        @Override
        public String call() throws Exception {
            Thread.sleep(timeToWait);
            System.out.println("Sleeping is over.");
            return result;
        }
    }
}
