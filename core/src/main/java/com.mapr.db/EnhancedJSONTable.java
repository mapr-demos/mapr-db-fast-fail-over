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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.mapr.db.Utils.getJsonTable;


public class EnhancedJSONTable implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EnhancedJSONTable.class);

    private static long MINUTE = 60000;

    private long timeOut;

    private AtomicReference<DocumentStore[]> documnetStoreHolder;

    /**
     * Variable for determining time that needed for switching table
     */
    private AtomicInteger counterForTableSwitching = new AtomicInteger(0);

    private ExecutorService tableOperationExecutor = Executors.newFixedThreadPool(2);

    /**
     * Scheduler for switching table
     */
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    /**
     * Scheduler for switching table to default order
     */
    private ScheduledExecutorService returnCounterToDefault = Executors.newScheduledThreadPool(1);

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is not successful in less than 500ms.
     *
     * @param primaryTable   the primary table used by the applicatin
     * @param secondaryTable the table used in case of fail over
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable) {
        this(primaryTable, secondaryTable, 500);
    }

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is successful in the <code>timeout</code>
     *
     * @param primaryTable   the primary table used by the applicatin
     * @param secondaryTable the table used in case of fail over
     * @param timeOut        the time out on primary table before switching to secondary.
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable, long timeOut) {
        DocumentStore primary = getJsonTable(primaryTable);
        DocumentStore secondary = getJsonTable(secondaryTable);
        this.timeOut = timeOut;

        this.documnetStoreHolder = new AtomicReference<>(new DocumentStore[]{primary, secondary});

        // Create task for reset time for switching table to default value,
        // that is executions will commence after 2 min then 2 min + 2 min}, then
        // 2 min + 2 min * 2}, and so on.
        returnCounterToDefault.scheduleAtFixedRate(
                () -> {
                    returnCounterToDefaultValue();
                    LOG.info("Counter return to default value!");
                }, 2 * MINUTE, 2 * MINUTE, TimeUnit.MILLISECONDS);
    }

    /**
     * See OJAI insert
     *
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
     *
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
     *
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
     * - call find on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     *
     * @return the documentstream coming from primary or secondary table
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private DocumentStream tryFind() throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documnetStoreHolder.get()[0].find(),
                () -> documnetStoreHolder.get()[1].find());
    }

    /**
     * This private method:
     * - call find on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     *
     * @param query
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private DocumentStream tryFind(QueryCondition query) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documnetStoreHolder.get()[0].find(query),
                () -> documnetStoreHolder.get()[1].find(query));
    }

    /**
     * This private method:
     * - call insert on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     *
     * @param document the document to insert in the DB
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void tryInsert(Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> {
            documnetStoreHolder.get()[0].insert(document);
            LOG.info("Operation performed with primary table");
        }, () -> {
            documnetStoreHolder.get()[1].insert(document);
            LOG.info("Operation performed with secondary table");
        });
    }

    private void performRetryLogic(Runnable primaryTask, Runnable secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<Void> primaryFuture = CompletableFuture.runAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            return null;
        });

        try {
            primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {


            LOG.info("Try to perform operation with secondary table");
            CompletableFuture<Void> secondaryFuture = CompletableFuture.runAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                return null;
            });

            secondaryFuture.acceptEitherAsync(primaryFuture, s -> {
            });

            // Prepare the system to swtich back to primary
            // this method contain the logic to define when to switch back
            int numberOfSwitch = counterForTableSwitching.getAndIncrement();
            createAndExecuteTaskForSwitchingTable(getTimeOut(numberOfSwitch));
        }
    }

    private <R> R performRetryLogicWithOutputData(Supplier<R> primaryTask, Supplier<R> secondaryTask)
            throws ExecutionException, InterruptedException {

        CompletableFuture<R> primaryFuture = CompletableFuture.supplyAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table ", throwable);
            throw new RetryPolicyException(throwable);
        });

        try {
            return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.error("Processing request to primary table too long");
            LOG.info("Try to perform operation with secondary table");

            CompletableFuture<R> secondaryFuture = CompletableFuture.supplyAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RetryPolicyException(throwable);
            });

            // Prepare the system to swtich back to primary
            // this method contain the logic to define when to switch back
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
        while (true) {
            DocumentStore[] origin = documnetStoreHolder.get();
            DocumentStore[] swapped = new DocumentStore[]{origin[1], origin[0]};
            if (documnetStoreHolder.compareAndSet(origin, swapped)) {
                return;
            }
        }
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
        documnetStoreHolder.get()[0].close();
        documnetStoreHolder.get()[1].close();
    }
}