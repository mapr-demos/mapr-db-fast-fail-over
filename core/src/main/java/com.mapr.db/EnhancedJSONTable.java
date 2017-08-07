package com.mapr.db;

import com.mapr.db.exception.RetryPolicyException;
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

import static com.mapr.db.Util.createAndExecuteTaskForSwitchingTableBack;
import static com.mapr.db.Util.getJsonTable;

public class EnhancedJSONTable {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedJSONTable.class);

    private DocumentStore primary;
    private DocumentStore secondary;

    private String primaryStorePath;
    private String secondatyStorePath;

    private long timeOut;

    private AtomicBoolean switched =
            new AtomicBoolean(false);


    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is not successful in less than 500ms.
     * @param primaryTable the primary table used by the applicatin
     * @param secondaryTable the table used in case of fail over
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable) {
        this(primaryTable,secondaryTable,500);
    }


  /**
   * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
   * to the secondary table if the operation on primary is successful in the <code>timeout</code>
   * @param primaryTable the primary table used by the applicatin
   * @param secondaryTable the table used in case of fail over
   * @param timeOut the time out on primary table before switching to secondary.
   */
  public EnhancedJSONTable(String primaryTable, String secondaryTable,
                             long timeOut) {
        this.primary = getJsonTable(primaryTable);
        this.primaryStorePath = primaryTable;
        this.secondary = getJsonTable(secondaryTable);
        this.secondatyStorePath = secondaryTable;
        this.timeOut = timeOut;
    }

  /**
   *  See OJAI insert
   * @param document
   */
  public void insert(Document document) {
        try {
            tryInsert(document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }


  /**
   * See OJAI find
   * @return
   */
  public DocumentStream find() {
        try {
            return tryFind();
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }


  /**
   * See OJAI find
   * @param query
   * @return
   */
    public DocumentStream find(QueryCondition query) {
        try {
            return tryFind(query);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

  /**
   * This private method:
   *   - call find on primary table and secondary table if issue
   *    using the <code>performRetryLogicWithOutputData</code> method.
   *
   *  In case of fail over this method stays on the failover table.
   *
   * @return the documentstream coming from primary or secondary table
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   */
    private DocumentStream tryFind() throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Perform operation with second table, because primary too slow");
            return secondary.find();
        } else {
            return performRetryLogicWithOutputData(() -> primary.find(), () -> secondary.find());
        }
    }

   /**
   * This private method:
   *   - call find on primary table and secondary table if issue
   *    using the <code>performRetryLogicWithOutputData</code> method.
   *
   *  In case of fail over this method stays on the failover table.
   *
   * @param query
   * @return
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   */
    private DocumentStream tryFind(QueryCondition query) throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Perform operation with second table, because primary too slow");
            return secondary.find(query);
        } else {
            return performRetryLogicWithOutputData(() -> primary.find(query), () -> secondary.find(query));
        }
    }

  /**
   * This private method:
   *   - call insert on primary table and secondary table if issue
   *    using the <code>performRetryLogicWithOutputData</code> method.
   *
   *  In case of fail over this method stays on the failover table.
   *
   * @param document the document to insert in the DB
   * @throws IOException
   * @throws InterruptedException
   * @throws ExecutionException
   */
    private void tryInsert(Document document) throws IOException, InterruptedException, ExecutionException {
        if (isTableSwitched()) {
            LOG.info("Operation switched to Secondary Table {}", this.secondatyStorePath);
            secondary.insert(document);
        } else {
            performRetryLogic(() -> {
                primary.insert(document);
                LOG.info("Operation performed with primary table");
            }, () -> {
                secondary.insert(document);
                LOG.warn("Operation Failed-Over to {} - TimeOut ", this.secondatyStorePath);
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
            throw new RetryPolicyException(throwable);
        });

        try {
            primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            LOG.info("Switched table to secondary for a 1000 ms");

            switched.set(true);
            // Prepare the system to swtich back to primary
            // this method contain the logic to define when to switch back
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

    private <R> R performRetryLogicWithOutputData(Supplier<R> primaryTask,
                                                  Supplier<R> secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<R> primaryFuture =
                CompletableFuture.supplyAsync(primaryTask);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            throw new RetryPolicyException(throwable);
        });

        try {
            return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            //LOG.info("Switched table to secondary for a 1000 ms");

            switched.set(true);
            // Prepare the system to swtich back to primary
            // this method contain the logic to define when to switch back
            createAndExecuteTaskForSwitchingTableBack(switched);

            LOG.info("Try to perform operation with secondary table");
            CompletableFuture<R> secondaryFuture =
                    CompletableFuture.supplyAsync(secondaryTask);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RetryPolicyException(throwable);
            });
            return secondaryFuture.join();
        }
    }

    private boolean isTableSwitched() {
        return switched.get();
    }
}