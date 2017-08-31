package com.mapr.db;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ojai.annotation.API.NonNullable;

/**
 * EnhancedJSONTable represents a wrapper above {@link DocumentStore} providing a fail-over
 * strategy that should provide user a high availability of cluster.
 * For update operations failover is not supported because of consistency problems especially with increment operations.
 */
public class EnhancedJSONTable implements DocumentStore {
    private static final Logger LOG = LoggerFactory.getLogger(EnhancedJSONTable.class);

    private static final String DB_DRIVER_NAME = "ojai:mapr:";

    /**
     * Variable that indicates that request is safe for failover
     */
    private static final boolean SAFE = true;

    private long timeOut;              // How long to wait before starting secondary query
    private long secondaryTimeOut;     // How long to wait before giving up on a good result

    private DocumentStore[] stores;    // the tables we talk to. Primary is first, then secondary
    private AtomicInteger current =    // the index for stores
            new AtomicInteger(0);

    private AtomicBoolean switched =
            new AtomicBoolean(false); // Indicates if the table switched in that moment

    private String[] tableNames;       // the names of the tables

    /**
     * Executor for working with primary store
     */
    private ExecutorService primaryExecutor = Executors.newSingleThreadExecutor();
    /**
     * Executor for working with failover store
     */
    private ExecutorService secondaryExecutor = Executors.newSingleThreadExecutor();
    /**
     * Variable for determining time that needed for switching table
     */
    private AtomicInteger counterForTableSwitching = new AtomicInteger(0);

    /**
     * Service that schedules failback operations
     */
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Do we need use failover for medium dangerous operations with db.
     * If true than we perform failover for this operations.
     */
    private boolean mediumDangerous = false;

    /**
     * Do we need use failover for non idempotent operations with db
     * If true than we perform failover for this operations.
     */
    private boolean veryDangerous = false;

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is not successful in less than 500ms.
     *
     * @param primaryTable   the primary table used by the application
     * @param secondaryTable the table used in case of fail over
     * @param medium         the flag that needed for determining what to do with medium dangerous operations, by default false
     * @param hard           the flag that needed for determining what to do with non idempotent operations, by default false
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable, long timeOut, boolean medium, boolean hard) {
        this(primaryTable, secondaryTable, timeOut);
        this.mediumDangerous = medium;
        this.veryDangerous = hard;
    }

    /**
     * Create a new JSON store that with a primary table and secondary table. The application will automatically switch
     * to the secondary table if the operation on primary is not successful in less than 500ms.
     *
     * @param primaryTable   the primary table used by the application
     * @param secondaryTable the table used in case of fail over
     */
    public EnhancedJSONTable(String primaryTable, String secondaryTable) {
        this(primaryTable, secondaryTable, 700);
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
        this.secondaryTimeOut = 15 * timeOut;

        DocumentStore primary = getDocumentStore(primaryTable);
        DocumentStore secondary = getDocumentStore(secondaryTable);

        this.stores = new DocumentStore[]{primary, secondary};
    }

    public boolean isMediumDangerous() {
        return mediumDangerous;
    }

    public void setMediumDangerous(boolean mediumDangerous) {
        this.mediumDangerous = mediumDangerous;
    }

    public boolean isVeryDangerous() {
        return veryDangerous;
    }

    public void setVeryDangerous(boolean veryDangerous) {
        this.veryDangerous = veryDangerous;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() {
        return checkAndDoWithFailover(DocumentStore::isReadOnly, SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws StoreException {
        doNoReturn(DocumentStore::flush, SAFE);  // TODO verify that this method reference does what is expected
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beginTrackingWrites() throws StoreException {
        doNoReturn(DocumentStore::beginTrackingWrites, veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beginTrackingWrites(@NonNullable String previousWritesContext) throws StoreException {
        doNoReturn((DocumentStore t) -> t.beginTrackingWrites(previousWritesContext), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String endTrackingWrites() throws StoreException {
        return checkAndDoWithFailover(DocumentStore::endTrackingWrites, veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearTrackedWrites() throws StoreException {
        doNoReturn(DocumentStore::clearTrackedWrites, veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String _id) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(_id), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value _id) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(_id), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String _id, String... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String _id, FieldPath... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value _id, String... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(_id, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value value, FieldPath... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(value, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String s, QueryCondition queryCondition) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(s, queryCondition), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value value, QueryCondition queryCondition) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(value, queryCondition), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String s, QueryCondition queryCondition, String... strings) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(s, queryCondition, strings), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(String s, QueryCondition queryCondition, FieldPath... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(s, queryCondition, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value value, QueryCondition queryCondition, String... strings) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(value, queryCondition, strings), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Document findById(Value value, QueryCondition queryCondition, FieldPath... fieldPaths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findById(value, queryCondition, fieldPaths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find() throws StoreException {
        return checkAndDoWithFailover(DocumentStore::find, SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find(@NonNullable String... paths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.find(paths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find(@NonNullable FieldPath... paths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.find(paths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find(@NonNullable QueryCondition c) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.find(c), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find(@NonNullable QueryCondition c, @NonNullable String... paths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.find(c, paths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream find(@NonNullable QueryCondition c, @NonNullable FieldPath... paths) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.find(c, paths), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream findQuery(@NonNullable Query query) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findQuery(query), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentStream findQuery(@NonNullable String query) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.findQuery(query), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable Value _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(_id, doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable Document doc, @NonNullable FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable Document doc, @NonNullable String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(doc, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable DocumentStream stream, @NonNullable FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable DocumentStream stream, @NonNullable String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insertOrReplace(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertOrReplace(@NonNullable String _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable String _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(@NonNullable Value _id, @NonNullable DocumentMutation m) throws StoreException {
        doNoReturn((DocumentStore t) -> t.update(_id, m), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(@NonNullable String _id, @NonNullable DocumentMutation mutation) throws StoreException {
        doNoReturn((DocumentStore t) -> t.update(_id, mutation), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable String _id) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(_id), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable Value _id) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(_id), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable Document doc, @NonNullable FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable Document doc, @NonNullable String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.delete(doc, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable DocumentStream stream, @NonNullable FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@NonNullable DocumentStream stream, @NonNullable String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.delete(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable Value _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(_id, doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable Document doc, @NonNullable FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable Document doc, @NonNullable String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.insert(doc, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable DocumentStream stream, @NonNullable FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(@NonNullable DocumentStream stream, @NonNullable String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.insert(stream, fieldAsKey), SAFE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable String _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(_id, doc), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable Value _id, @NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(_id, doc), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable Document doc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable Document doc, @NonNullable FieldPath fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc, fieldAsKey), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable Document doc, @NonNullable String fieldAsKey) throws StoreException {
        doNoReturn((DocumentStore t) -> t.replace(doc, fieldAsKey), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable DocumentStream stream) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable DocumentStream stream, @NonNullable FieldPath fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream, fieldAsKey), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void replace(@NonNullable DocumentStream stream, @NonNullable String fieldAsKey) throws MultiOpException {
        doNoReturn((DocumentStore t) -> t.replace(stream, fieldAsKey), mediumDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, byte inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, short inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, int inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, long inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, float inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, double inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable String _id, @NonNullable String field, @NonNullable BigDecimal inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, byte inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, short inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, int inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, long inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, float inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, double inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(@NonNullable Value _id, @NonNullable String field, @NonNullable BigDecimal inc) throws StoreException {
        doNoReturn((DocumentStore t) -> t.increment(_id, field, inc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(@NonNullable String _id, @NonNullable QueryCondition condition,
                                  @NonNullable DocumentMutation mutation) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndMutate(_id, condition, mutation), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(@NonNullable String _id, @NonNullable QueryCondition condition) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndDelete(_id, condition), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndReplace(@NonNullable String _id, @NonNullable QueryCondition condition,
                                   @NonNullable Document doc) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndReplace(_id, condition, doc), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(@NonNullable Value _id, @NonNullable QueryCondition condition,
                                  @NonNullable DocumentMutation m) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndMutate(_id, condition, m), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(@NonNullable Value _id, @NonNullable QueryCondition condition) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndDelete(_id, condition), veryDangerous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkAndReplace(@NonNullable Value _id, @NonNullable QueryCondition condition, @NonNullable Document doc) throws StoreException {
        return checkAndDoWithFailover((DocumentStore t) -> t.checkAndReplace(_id, condition, doc), veryDangerous);
    }

    public boolean isTableSwitched() {
        return switched.get();
    }

    private void doNoReturn(TableProcedure task, boolean withFailover) {
        checkAndDoWithFailover((DocumentStore t) -> {
            task.apply(t);
            return null;
        }, withFailover);
    }

    private <R> R checkAndDoWithFailover(TableFunction<R> task, boolean withFailover) {
        int i = current.get();
        DocumentStore primary = stores[i];
        DocumentStore secondary = stores[1 - i];
        if (withFailover) {
            if (switched.get()) {
                // change executor if table switched, in this case primary executor work only with primary cluster
                // and secondary executor work only with failover table
                return doWithFallback(secondaryExecutor, primaryExecutor, timeOut, secondaryTimeOut, task, primary, secondary,
                        this::swapTableLinks, switched);
            } else {
                return doWithFallback(primaryExecutor, secondaryExecutor, timeOut, secondaryTimeOut, task, primary, secondary,
                        this::swapTableLinks, switched);
            }
        } else {
            return doWithoutFailover(task, primary);
        }
    }

    /**
     * Process request to db without Failover
     *
     * @param task    A lambda with one argument, a table, that does the desired operation
     * @param primary The primary table
     * @param <R>     The type that task will return
     * @return The value returned by task
     */
    private <R> R doWithoutFailover(TableFunction<R> task, DocumentStore primary) {
        try {
            return primaryExecutor.submit(() -> task.apply(primary)).get();
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
        }
    }

    /**
     * Tries to do task on primary until timeOut milliseconds have passed. From then
     * the task is also attempted with secondary. If second succeeds, we use that result.
     * If the primary blows the first timeout, then we initiate a failover by invoking
     * failoverTask. When both primary and secondary throw exceptions, we rethrow the
     * last exception received. When secondary exceed secondaryTimeOut
     * milliseconds with no exceptions and no results, then an exception is thrown.
     * <p>
     * This method is static to make testing easier.
     *
     * @param prim             The executor that does all the work
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
    static <R> R doWithFallback(ExecutorService prim, ExecutorService sec,
                                long timeOut, long secondaryTimeOut,
                                TableFunction<R> task,
                                DocumentStore primary, DocumentStore secondary,
                                Runnable failover, AtomicBoolean switched) {
        Future<R> primaryFuture = prim.submit(() -> task.apply(primary));
        try {
            try {
                // try on the primary table ... if we get a result, we win
                return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException e) {
                // We have lost confidence in the primary at this point even if we get a result
                // We cancel request to the primary table, for fast change to the failover table
                primaryFuture.cancel(true);
                if (!switched.get()) {
                    failover.run();
                }
                // No result in time from primary so we now try on secondary.
                // Exceptional returns when timeout expires.
                return sec.submit(() -> task.apply(secondary)).get(secondaryTimeOut, TimeUnit.MILLISECONDS);
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

    /**
     * Create task for swapping table back after timeout
     *
     * @param timeout Time after what we swap table back
     */
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
                return minute / 3;
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
        primaryExecutor.shutdownNow();
        secondaryExecutor.shutdownNow();
        try {
            stores[0].close();
        } finally {
            stores[1].close();
        }
    }

    /**
     * Get DocumentStore from MapR-DB.
     * Table must exist.
     *
     * @param tableName Name that correspond to db table name
     * @return com.mapr.db.Table
     */
    private DocumentStore getDocumentStore(String tableName) {
        Connection connection = DriverManager.getConnection(DB_DRIVER_NAME);
        return connection.getStore(tableName);
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