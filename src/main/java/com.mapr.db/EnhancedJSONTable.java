package com.mapr.db;

import org.ojai.Document;
import org.ojai.store.DocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mapr.db.Util.createAndExecuteTaskForSwitchingTableBack;
import static com.mapr.db.Util.getJsonTable;

public class EnhancedJSONTable {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedBinaryTable.class);

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
            createAndExecuteTaskForSwitchingTableBack(switched);

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

    private boolean isTableSwitched() {
        return switched.get();
    }
}
