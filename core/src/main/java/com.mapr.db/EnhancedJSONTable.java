package com.mapr.db;

import com.google.common.collect.Lists;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.annotation.API;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.Utils.getDocumentStore;

/**
 * EnhancedJSONTable represents a wrapper above {@link DocumentStore} providing a fail-over
 * strategy that should provide user a high availability of cluster.
 * For update operations failover is not supported because of consistency problems especially with increment operations.
 */
public class EnhancedJSONTable implements DocumentStore {
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedJSONTable.class);

    private long timeOut;              // How long to wait before starting secondary query
    private long secondaryTimeOut;     // How long to wait before giving up on a good result

    private DocumentStore[] stores;    // the tables we talk to. Primary is first, then secondary
    private AtomicInteger current =    // the index for stores
            new AtomicInteger(0);

    private AtomicBoolean switched =
            new AtomicBoolean(false); // Indicates if the table switched in that moment

    private String[] tableNames;       // the names of the tables

    // thread pool that does all the work
    private ExecutorService tableOperationExecutor = Executors.newFixedThreadPool(2);

    // TODO make fail back work
    /**
     * Variable for determining time that needed for switching table
     */
    private AtomicInteger counterForTableSwitching = new AtomicInteger(0);

    /**
     * Service that schedules failback operations
     */
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is not successful in less than 500ms.
     *
     * @param primaryTable   the primary table used by the application
     * @param secondaryTable the table used in case of fail over
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable) {
        this(primaryTable, secondaryTable, 500);
    }

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is successful in the <code>timeout</code>
     *
     * @param primaryTable   the primary table used by the application
     * @param secondaryTable the table used in case of fail over
     * @param timeOut        the time out on primary table before switching to secondary.
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable, long timeOut) {
        this.tableNames = new String[]{primaryTable, secondaryTable};
        this.timeOut = timeOut;
        this.secondaryTimeOut = 15 * timeOut; // TODO (related to #15) Find the way to reduce time for first request to secondary cluster

        DocumentStore primary = getDocumentStore(primaryTable);
        DocumentStore secondary = getDocumentStore(secondaryTable);

        this.stores = new DocumentStore[]{primary, secondary};
    }

    @Override
    public boolean isReadOnly() {
        return doWithFailover(DocumentStore::isReadOnly);
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#flush()
     */
    @Override
    public void flush() throws StoreException {
        doNoReturn(DocumentStore::flush);  // TODO verify that this method reference does what is expected
    }

    @Override
    public void beginTrackingWrites() throws StoreException {
        doNoReturn(DocumentStore::beginTrackingWrites);
    }

    @Override
    public void beginTrackingWrites(@API.NonNullable String previousWritesContext) throws StoreException {
        doNoReturn((DocumentStore t) -> t.beginTrackingWrites(previousWritesContext));
    }

    @Override
    public String endTrackingWrites() throws StoreException {
        return doWithFailover(DocumentStore::endTrackingWrites);
    }

    @Override
    public void clearTrackedWrites() throws StoreException {
        doNoReturn(DocumentStore::clearTrackedWrites);
    }

    @Override
    public Document findById(String _id) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findById(_id));
    }

    @Override
    public Document findById(Value _id) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findById(_id));
    }

    @Override
    public Document findById(String _id, String... fieldPaths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths));
    }

    @Override
    public Document findById(String _id, FieldPath... fieldPaths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths));
    }

    @Override
    public Document findById(Value _id, String... fieldPaths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths));
    }

    @Override
    public Document findById(Value value, FieldPath... fieldPaths) throws StoreException {
        return null;
    }

    @Override
    public Document findById(String s, QueryCondition queryCondition) throws StoreException {
        return null;
    }

    @Override
    public Document findById(Value value, QueryCondition queryCondition) throws StoreException {
        return null;
    }

    @Override
    public Document findById(String s, QueryCondition queryCondition, String... strings) throws StoreException {
        return null;
    }

    @Override
    public Document findById(String s, QueryCondition queryCondition, FieldPath... fieldPaths) throws StoreException {
        return null;
    }

    @Override
    public Document findById(Value value, QueryCondition queryCondition, String... strings) throws StoreException {
        return null;
    }

    @Override
    public Document findById(Value value, QueryCondition queryCondition, FieldPath... fieldPaths) throws StoreException {
        return null;
    }

    @Override
    public DocumentStream find() throws StoreException {
        return doWithFailover(DocumentStore::find);
    }

    @Override
    public DocumentStream find(String... paths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.find(paths));
    }

    @Override
    public DocumentStream find(FieldPath... paths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.find(paths));
    }

    @Override
    public DocumentStream find(QueryCondition c) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.find(c));
    }

    @Override
    public DocumentStream find(QueryCondition c, String... paths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.find(c, paths));
    }

    @Override
    public DocumentStream find(QueryCondition c, FieldPath... paths) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.find(c, paths));
    }

    @Override
    public DocumentStream findQuery(@API.NonNullable Query query) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findQuery(query));
    }

    @Override
    public DocumentStream findQuery(@API.NonNullable String query) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.findQuery(query));
    }

    @Override
    public void insertOrReplace(Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc));
    }

    @Override
    public void insertOrReplace(Value _id, Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(_id, doc));
    }

    @Override
    public void insertOrReplace(Document doc, FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc, fieldAsKey));
    }

    @Override
    public void insertOrReplace(Document doc, String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc, fieldAsKey));
    }

    @Override
    public void insertOrReplace(DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream));
    }

    @Override
    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream, fieldAsKey));
    }

    @Override
    public void insertOrReplace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream, fieldAsKey));
    }

    @Override
    public void insertOrReplace(@API.NonNullable String _id, @API.NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc));
    }

    @Override
    public void insert(@API.NonNullable String _id, @API.NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc));
    }

    @Override
    public void update(Value _id, DocumentMutation m) throws StoreException {
        doNoReturn((DocumentStore t) -> t.update(_id, m));
    }

    @Override
    public void update(@API.NonNullable String _id, @API.NonNullable DocumentMutation mutation) throws StoreException {
        doNoReturn((DocumentStore t) -> t.update(_id, mutation));
    }

    @Override
    public void delete(@API.NonNullable String _id) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(_id));
    }

    @Override
    public void delete(Value _id) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(_id));
    }

    @Override
    public void delete(Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc));
    }

    @Override
    public void delete(Document doc, FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc, fieldAsKey));
    }

    @Override
    public void delete(Document doc, String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc, fieldAsKey));
    }

    @Override
    public void delete(DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream));
    }

    @Override
    public void delete(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream, fieldAsKey));
    }

    @Override
    public void delete(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream, fieldAsKey));
    }

    @Override
    public void insert(Value _id, Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc));
    }

    @Override
    public void insert(Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc));
    }

    @Override
    public void insert(Document doc, FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc));
    }

    @Override
    public void insert(Document doc, String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc, fieldAsKey));
    }

    @Override
    public void insert(DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream));
    }

    @Override
    public void insert(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream, fieldAsKey));
    }

    @Override
    public void insert(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream, fieldAsKey));
    }

    @Override
    public void replace(@API.NonNullable String _id, @API.NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(_id, doc));
    }

    @Override
    public void replace(Value _id, Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(_id, doc));
    }

    @Override
    public void replace(Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc));
    }

    @Override
    public void replace(Document doc, FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc, fieldAsKey));
    }

    @Override
    public void replace(Document doc, String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc, fieldAsKey));
    }

    @Override
    public void replace(DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream));
    }

    @Override
    public void replace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream, fieldAsKey));
    }

    @Override
    public void replace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream, fieldAsKey));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, byte inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, short inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, int inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, long inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, float inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, double inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(@API.NonNullable String _id, @API.NonNullable String field, @API.NonNullable BigDecimal inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, byte inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, short inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, int inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, long inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, float inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, double inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public void increment(Value _id, String field, BigDecimal inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc));
    }

    @Override
    public boolean checkAndMutate(@API.NonNullable String _id, @API.NonNullable QueryCondition condition, @API.NonNullable DocumentMutation mutation) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndMutate(_id, condition, mutation));
    }

    @Override
    public boolean checkAndDelete(@API.NonNullable String _id, @API.NonNullable QueryCondition condition) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndDelete(_id, condition));
    }

    @Override
    public boolean checkAndReplace(@API.NonNullable String _id, @API.NonNullable QueryCondition condition, @API.NonNullable Document doc) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndReplace(_id, condition, doc));
    }

    @Override
    public boolean checkAndMutate(Value _id, QueryCondition condition, DocumentMutation m) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndMutate(_id, condition, m));
    }

    @Override
    public boolean checkAndDelete(Value _id, QueryCondition condition) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndDelete(_id, condition));
    }

    @Override
    public boolean checkAndReplace(Value _id, QueryCondition condition, Document doc) throws StoreException {
        return doWithFailover((DocumentStore t) -> t.checkAndReplace(_id, condition, doc));
    }

    private void doNoReturn(TableProcedure task) {
        doWithFailover((DocumentStore t) -> {
            task.apply(t);
            return null;
        });
    }

    private <R> R doWithFailover(TableFunction<R> task) {
        int i = current.get();
        DocumentStore primary = stores[i];
        DocumentStore secondary = stores[1 - i];
        return doWithFallback(tableOperationExecutor, timeOut, secondaryTimeOut, task, primary, secondary, this::swapTableLinks, switched);
    }

    /**
     * Tries to do task on primary until timeOut milliseconds have passed. From then
     * the task is also attempted with secondary. If either succeeds, we use that result.
     * If the primary blows the first timeout, then we initiate a failover by invoking
     * failoverTask. When both primary and secondary throw exceptions, we rethrow the
     * last exception received. When both primary and secondary exceed secondaryTimeOut
     * milliseconds with no exceptions and no results, then an exception is thrown.
     * <p>
     * This method is static to make testing easier.
     *
     * @param exec             The executor that does all the work
     * @param timeOut          How long to wait before invoking the secondary
     * @param secondaryTimeOut How long to wait before entirely giving up
     * @param task             A lambda with one argument, a table, that does the desired operation
     * @param primary          The primary table
     * @param secondary        The secondary table
     * @param failover         The function to call when primary doesn't respond quickly
     * @param switched         The flag, that indicate that table switched or not
     * @param <R>              The type that task will return
     * @return The value returned by task
     * @throws StoreException    If both primary and secondary fail
     * @throws FailoverException If both primary and secondary fail. This may wrap a real exception
     */
    static <R> R doWithFallback(ExecutorService exec,
                                long timeOut, long secondaryTimeOut,
                                TableFunction<R> task,
                                DocumentStore primary, DocumentStore secondary,
                                Runnable failover, AtomicBoolean switched) {
        Future<R> primaryFuture = exec.submit(() -> task.apply(primary));
        try {
            try {
                // try on the primary table ... if we get a result, we win
                return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // No result in time from primary so we now try on either primary or secondary.
                // Whichever returns first is the winner and the other is cancelled.
                // Exceptional returns will be held until the other task completes successfully
                // or the timeout expires.
                @SuppressWarnings("unchecked")
                List<Callable<R>> tasks = Lists.newArrayList(
                        primaryFuture::get,
                        () -> task.apply(secondary)
                );
                // We have lost confidence in the primary at this point even if we get a result
                if (!switched.get()) {
                    failover.run();
                }
                return exec.invokeAny(tasks, secondaryTimeOut, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            // this should never happen except perhaps in debugging or on shutdown
            throw new FailoverException("Thread was interrupted during operation", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // these are likely StoreException, but we don't differentiate
                throw (RuntimeException) cause;
            } else {
                // this should not happen in our situation since none of the methods do this
                throw new FailoverException("Checked exception thrown (shouldn't happen)", cause);
            }
        } catch (TimeoutException e) {
            throw new FailoverException("Operation timed out on primary and secondary tables", e);
        }
    }

    /**
     * Swap primary and secondary tables
     * <p>
     * When failing over to another table/cluster or when going back to origin/master cluster
     * we do not change the whole logic, but simply switch the primary/secondary tables
     */
    private void swapTableLinks() {
        current.getAndUpdate(old -> 1 - old);
        switched.compareAndSet(switched.get(), !switched.get());
        LOG.info("Table switched: " + switched.get());
        if (switched.get()) {
            int stick = counterForTableSwitching.getAndIncrement();
            LOG.info("Switch table for - {} ms", getTimeOut(stick));
            swapTableBackAfter(getTimeOut(stick));
        }
    }

    private void swapTableBackAfter(long timeout) {
        scheduler.schedule(
                this::swapTableLinks, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Determines the amount of time that we need to stay on another table
     * <p>
     *
     * @param numberOfSwitch Quantity of table switching
     * @return time in milliseconds
     */
    private long getTimeOut(int numberOfSwitch) {
        long minute = 60000;
        switch (numberOfSwitch) {
            case 0:
                return minute / 6;
            case 1:
                return minute;
            default:
                return 2 * minute;
        }
    }

    /**
     * Shutdown all executors, close connection to the tables.
     * If you do not call this method, the application will freeze
     *
     * @throws StoreException If the underlying tables fail to close cleanly.
     */
    @Override
    public void close() throws StoreException {
        scheduler.shutdownNow();
        tableOperationExecutor.shutdownNow();
        try {
            stores[0].close();
        } finally {
            stores[1].close();
        }
    }

    static class FailoverException extends StoreException {
        FailoverException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    public interface TableFunction<R> {
        R apply(DocumentStore d) throws InterruptedException;
    }

    public interface TableProcedure {
        void apply(DocumentStore t);
    }
}