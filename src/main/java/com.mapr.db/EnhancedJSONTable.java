package com.mapr.db;

import com.mapr.db.exception.RetryPolicyException;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentStore;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.mapr.db.Util.getJsonTable;

//import static com.mapr.db.Util.createAndExecuteTaskForSwitchingTableBack;

public class EnhancedJSONTable implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedJSONTable.class);

    private static long MINUTE = 60000;

    private DocumentStore primary;
    private DocumentStore secondary;

    private long timeOut;

    /**
     * Variable for determining time that needed for switching table
     */
    private AtomicInteger counterForTableSwitching =
            new AtomicInteger(0);

    private ExecutorService tableOperationExecutor =
            Executors.newFixedThreadPool(2);

    /**
     * Scheduler for switching table
     */
    private ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);
    /**
     * Scheduler for switching table to default order
     */
    private ScheduledExecutorService returnCounterToDefault =
            Executors.newScheduledThreadPool(1);

    public EnhancedJSONTable(String primaryTable, String secondaryTable,
                             long timeOut) {
        this.primary =
                getJsonTable(primaryTable);
        this.secondary =
                getJsonTable(secondaryTable);
        this.timeOut = timeOut;

        // Create task for reset time for switching table to default value,
        // that is executions will commence after 2 min then 2 min + 2 min}, then
        // 2 min + 2 min * 4}, and so on.

        returnCounterToDefault.scheduleAtFixedRate(
                () -> {
                    returnCounterToDefaultValue();
                    LOG.info("Counter return to default value!");
                }, 4 * MINUTE, 4 * MINUTE, TimeUnit.MILLISECONDS);
    }

    public void insert(Document document) {
        try {
            tryInsert(document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    public DocumentStream find() {
        try {
            return tryFind();
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    public DocumentStream find(QueryCondition query) {
        try {
            return tryFind(query);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    private DocumentStream tryFind() throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> primary.find(), () -> secondary.find());
    }

    private DocumentStream tryFind(QueryCondition query) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> primary.find(query), () -> secondary.find(query));
    }

    private void tryInsert(Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> {
            primary.insert(document);
            LOG.info("Operation performed with primary table");
        }, () -> {
            secondary.insert(document);
            LOG.info("Operation performed with secondary table");
        });
    }

    private void performRetryLogic(Runnable primaryTask,
                                   Runnable secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<Void> primaryFuture =
                CompletableFuture.runAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            return null;
        });

        try {
            primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {


            LOG.info("Try to perform operation with secondary table");
            CompletableFuture<Void> secondaryFuture =
                    CompletableFuture.runAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                return null;
//                throw new RuntimeException(throwable);
            });

            secondaryFuture.acceptEitherAsync(primaryFuture, s -> {
                if (secondaryFuture.isDone()) {
                    primaryFuture.cancel(true);
                }
            });

            int numberOfSwitch = counterForTableSwitching.getAndIncrement();
            createAndExecuteTaskForSwitchingTable(getTimeOut(numberOfSwitch));
        }
    }

    private <R> R performRetryLogicWithOutputData(Supplier<R> primaryTask,
                                                  Supplier<R> secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<R> primaryFuture =
                CompletableFuture.supplyAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            throw new RetryPolicyException(throwable);
        });

        try {
            return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            LOG.info("Try to perform operation with secondary table");

            CompletableFuture<R> secondaryFuture =
                    CompletableFuture.supplyAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RetryPolicyException(throwable);
            });

            int numberOfSwitch = counterForTableSwitching.getAndIncrement();
            createAndExecuteTaskForSwitchingTable(getTimeOut(numberOfSwitch));

            return secondaryFuture.join();
        }
    }

    /**
     * Swap primary and secondary tables
     *
     * @param timeForSwitchingTableBack Time to return to the initial state
     */
    private void createAndExecuteTaskForSwitchingTable(long timeForSwitchingTableBack) {
        LOG.info("Switch table for - {} ms", timeForSwitchingTableBack);
        swapTableLinks();
        scheduler.schedule(
                () -> {
                    swapTableLinks();
                    LOG.info("TABLE SWITCHED BACK");
                }, timeForSwitchingTableBack, TimeUnit.MILLISECONDS);
    }

    private void returnCounterToDefaultValue() {
        counterForTableSwitching.set(0);
    }

    /**
     * Determines the amount of time that we need to stay on another table
     *
     * @param numberOfSwitch Quantity of table switching
     * @return time in milliseconds
     */
    private long getTimeOut(int numberOfSwitch) {
        switch (numberOfSwitch) {
            case 0:
                return MINUTE / 2;
            case 1:
                return MINUTE;
            default:
                return 2 * MINUTE;
        }
    }

    /**
     * Swap primary and secondary tables
     */
    private void swapTableLinks() {
        DocumentStore tmp = primary;
        primary = secondary;
        secondary = tmp;
    }

    /**
     * Shutdown all executors, close connection to the tables.
     * If you do not call this method, the application will freeze
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        returnCounterToDefault.shutdownNow();
        scheduler.shutdownNow();
        tableOperationExecutor.shutdownNow();
        primary.close();
        secondary.close();
    }
}