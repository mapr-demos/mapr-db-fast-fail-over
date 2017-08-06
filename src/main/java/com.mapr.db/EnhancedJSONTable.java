package com.mapr.db;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentStore;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.mapr.db.Util.getJsonTable;

public class EnhancedJSONTable {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedJSONTable.class);

    private DocumentStore primary;
    private DocumentStore secondary;

    private long timeOut;

    private AtomicBoolean switched =
            new AtomicBoolean(false);

    public EnhancedJSONTable(String primaryClusterURL, String primaryTable,
                             String secondaryClusterURL, String secondaryTable, long timeOut) {
        this.primary =
                getJsonTable(primaryClusterURL, primaryTable);
        this.secondary =
                getJsonTable(secondaryClusterURL, secondaryTable);
        this.timeOut = timeOut;
    }

    public void insert(Document document) {
        try {
            tryInsert(document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            e.printStackTrace();
        }
    }

    public DocumentStream find() {
        try {
            return tryFind();
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public DocumentStream find(QueryCondition query) {
        try {
            return tryFind(query);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private DocumentStream tryFind() throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Perform operation with second table, because primary too slow");
            return secondary.find();
        } else {
            return performRetryLogicWithOutputData(() -> primary.find(), () -> secondary.find());
        }
    }

    private DocumentStream tryFind(QueryCondition query) throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Perform operation with second table, because primary too slow");
            return secondary.find(query);
        } else {
            return performRetryLogicWithOutputData(() -> primary.find(query), () -> secondary.find(query));
        }
    }

    private void tryInsert(Document document) throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Perform operation with second table, because primary too slow");
            secondary.insert(document);
        } else {
            performRetryLogic(() -> {
                primary.insert(document);
                LOG.info("Operation performed with primary table");
            }, () -> {
                secondary.insert(document);
                LOG.info("Operation performed with secondary table");
            });
        }
    }

    private void performRetryLogic(Runnable primaryTask,
                                   Runnable secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<Void> primaryFuture =
                CompletableFuture.runAsync(primaryTask);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            throw new RuntimeException(throwable);
        });

        try {
            primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            LOG.info("Switched table to secondary for a 1000 ms");

            switched.set(true);
            //createAndExecuteTaskForSwitchingTableBack(switched);

            LOG.info("Try to perform operation with secondary table");
            CompletableFuture<Void> secondaryFuture =
                    CompletableFuture.runAsync(secondaryTask);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RuntimeException(throwable);
            });
            secondaryFuture.acceptEitherAsync(primaryFuture, s -> {
            });
        }
    }

    private <R> R performRetryLogicWithOutputData(Supplier<R> primaryTask,
            Supplier<R> secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<R> primaryFuture =
                CompletableFuture.supplyAsync(primaryTask);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            throw new RuntimeException(throwable);
        });

        try {
            return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            //LOG.info("Switched table to secondary for a 1000 ms");

            switched.set(true);
            //createAndExecuteTaskForSwitchingTableBack(switched);

            LOG.info("Try to perform operation with secondary table");
            CompletableFuture<R> secondaryFuture =
                    CompletableFuture.supplyAsync(secondaryTask);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RuntimeException(throwable);
            });
            return secondaryFuture.join();
        }
    }

    private boolean isTableSwitched() {
        return switched.get();
    }
}