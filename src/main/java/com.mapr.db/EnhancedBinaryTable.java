package com.mapr.db;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mapr.db.Util.createAndExecuteTaskForSwitchingTableBack;
import static com.mapr.db.Util.getBinaryTable;

public class EnhancedBinaryTable {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedBinaryTable.class);

    private Table primary;
    private Table alternate;
    private RetryPolicy policy;

    private AtomicBoolean switched = new AtomicBoolean(false);

    public EnhancedBinaryTable(Connection primary, Connection alternate, RetryPolicy policy) {
        this.primary = getBinaryTable(primary, policy.getPrimaryTable());
        this.alternate = getBinaryTable(alternate, policy.getAlternateTable());
        this.policy = policy;
    }

    void put(Put put) throws IOException, InterruptedException, ExecutionException {
        if (!switched.get()) {
            CompletableFuture<Void> completableFuture =
                    CompletableFuture.supplyAsync(() -> {
                        putToTable(put);
                        return null;
                    });
            completableFuture.exceptionally(throwable -> {
                LOG.error("Problem", throwable);
                return null;
            });
            try {
                completableFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                switched.set(true);
                createAndExecuteTaskForSwitchingTableBack(switched);
                alternate.put(put);
            }
        } else {
            alternate.put(put);
        }
    }

    private void putToTable(Put put) {
        try {
            primary.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
