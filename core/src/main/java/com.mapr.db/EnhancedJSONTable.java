package com.mapr.db;

import com.mapr.db.exception.RetryPolicyException;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.mapr.db.Utils.getJsonTable;
import static org.ojai.annotation.API.NonNullable;

/**
 * EnhancedJSONTable represents a wrapper above {@link DocumentStore} providing a fail-over
 * strategy that should provide user a high availability of cluster.
 * For update operations failover is not supported because of consistency problems especially with increment operations.
 */
public class EnhancedJSONTable implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EnhancedJSONTable.class);

    private long timeOut;

    private AtomicBoolean switched = new AtomicBoolean(false);

    private AtomicReference<DocumentStore[]> documentStoreHolder;

    private String primaryTable;
    private String secondaryTable;

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
        this.primaryTable = primaryTable;
        this.secondaryTable = secondaryTable;
        this.timeOut = timeOut;

        DocumentStore primary = getJsonTable(primaryTable);
        DocumentStore secondary = getJsonTable(secondaryTable);

        this.documentStoreHolder = new AtomicReference<>(new DocumentStore[]{primary, secondary});
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#flush()
     */
    public void flush() {
        processWithFailover(() -> documentStoreHolder.get()[0].flush(),
                () -> documentStoreHolder.get()[1].flush());
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(String)
     */
    public Document findById(String _id) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id),
                () -> documentStoreHolder.get()[1].findById(_id));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(Value)
     */
    public Document findById(Value _id) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id),
                () -> documentStoreHolder.get()[1].findById(_id));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(String, String...)
     */
    public Document findById(String _id, String... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(String, QueryCondition)
     */
    public Document findById(String _id, QueryCondition condition) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition),
                () -> documentStoreHolder.get()[1].findById(_id, condition));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(Value, QueryCondition)
     */
    public Document findById(Value _id, QueryCondition condition) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition),
                () -> documentStoreHolder.get()[1].findById(_id, condition));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(String, QueryCondition, String...)
     */
    public Document findById(String _id, QueryCondition condition, String... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(String, QueryCondition, FieldPath...)
     */
    public Document findById(String _id, QueryCondition condition, FieldPath... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(Value, QueryCondition, String...)
     */
    public Document findById(Value _id, QueryCondition condition, String... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findById(Value, QueryCondition, FieldPath...)
     */
    public Document findById(Value _id, QueryCondition condition, FieldPath... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#find()
     */
    public DocumentStream find() {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].find(),
                () -> documentStoreHolder.get()[1].find());
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findQuery(Query)
     */
    public DocumentStream findQuery(Query query) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findQuery(query),
                () -> documentStoreHolder.get()[1].findQuery(query));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#findQuery(String)
     */
    public DocumentStream findQuery(String queryJSON) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].findQuery(queryJSON),
                () -> documentStoreHolder.get()[1].findQuery(queryJSON));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#find(String...)
     */
    public DocumentStream find(String... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].find(fieldPaths),
                () -> documentStoreHolder.get()[1].find(fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#find(FieldPath...)
     */
    public DocumentStream find(FieldPath... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].find(fieldPaths),
                () -> documentStoreHolder.get()[1].find(fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#find(QueryCondition)
     */
    public DocumentStream find(QueryCondition condition) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].find(condition),
                () -> documentStoreHolder.get()[1].find(condition));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#find(QueryCondition, String...)
     */
    public DocumentStream find(QueryCondition condition, String... fieldPaths) {
        return processWithFailoverWithOutputData(() -> documentStoreHolder.get()[0].find(condition, fieldPaths),
                () -> documentStoreHolder.get()[1].find(condition, fieldPaths));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(Document)
     */
    public void insertOrReplace(Document document) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(document),
                () -> documentStoreHolder.get()[1].insertOrReplace(document));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(String, Document)
     */
    public void insertOrReplace(String _id, Document document) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(_id, document),
                () -> documentStoreHolder.get()[1].insertOrReplace(_id, document));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(Value, Document)
     */
    public void insertOrReplace(Value _id, Document document) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(_id, document),
                () -> documentStoreHolder.get()[1].insertOrReplace(_id, document));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(Document, FieldPath)
     */
    public void insertOrReplace(Document document, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(document, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(document, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(Document, String)
     */
    public void insertOrReplace(Document document, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(document, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(document, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(DocumentStream)
     */
    public void insertOrReplace(DocumentStream stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(stream),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(DocumentStream, FieldPath)
     */
    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insertOrReplace(DocumentStream, String)
     */
    public void insertOrReplace(DocumentStream stream, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insertOrReplace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream, fieldAsKey));
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#update(String, DocumentMutation)
     */
    public void update(String _id, DocumentMutation mutation) {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].update(_id, mutation);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#update(Value, DocumentMutation)
     */
    public void update(Value _id, DocumentMutation mutation) {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].update(_id, mutation);
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(String)
     */
    public void delete(String _id) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(_id),
                () -> documentStoreHolder.get()[1].delete(_id));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(Value)
     */
    public void delete(Value _id) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(_id),
                () -> documentStoreHolder.get()[1].delete(_id));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(Document)
     */
    public void delete(Document doc) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(doc),
                () -> documentStoreHolder.get()[1].delete(doc));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(Document, FieldPath)
     */
    public void delete(Document doc, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(Document, String)
     */
    public void delete(Document doc, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(DocumentStream)
     */
    public void delete(DocumentStream stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(stream),
                () -> documentStoreHolder.get()[1].delete(stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(DocumentStream, FieldPath)
     */
    public void delete(DocumentStream stream, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#delete(DocumentStream, String)
     */
    public void delete(DocumentStream stream, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].delete(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(String, Document)
     */
    public void insert(String _id, Document stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(_id, stream),
                () -> documentStoreHolder.get()[1].insert(_id, stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(Value, Document)
     */
    public void insert(Value _id, Document stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(_id, stream),
                () -> documentStoreHolder.get()[1].insert(_id, stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(Document)
     */
    public void insert(Document document) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(document),
                () -> documentStoreHolder.get()[1].insert(document));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(Document, FieldPath)
     */
    public void insert(Document doc, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(Document, String)
     */
    public void insert(Document doc, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(DocumentStream)
     */
    public void insert(DocumentStream stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(stream),
                () -> documentStoreHolder.get()[1].insert(stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(DocumentStream, FieldPath)
     */
    public void insert(DocumentStream stream, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#insert(DocumentStream, String)
     */
    public void insert(DocumentStream stream, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].insert(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(String, Document)
     */
    public void replace(String _id, Document doc) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(_id, doc),
                () -> documentStoreHolder.get()[1].replace(_id, doc));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(Value, Document)
     */
    public void replace(Value _id, Document doc) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(_id, doc),
                () -> documentStoreHolder.get()[1].replace(_id, doc));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(Document)
     */
    public void replace(Document doc) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(doc),
                () -> documentStoreHolder.get()[1].replace(doc));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(Document, FieldPath)
     */
    public void replace(Document doc, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(Document, String)
     */
    public void replace(Document doc, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(doc, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(DocumentStream)
     */
    public void replace(DocumentStream stream) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(stream),
                () -> documentStoreHolder.get()[1].replace(stream));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(DocumentStream, FieldPath)
     */
    public void replace(DocumentStream stream, FieldPath fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(stream, fieldAsKey));
    }

    /**
     * Performs operation with fail-over
     *
     * @see DocumentStore#replace(DocumentStream, String)
     */
    public void replace(DocumentStream stream, String fieldAsKey) {
        processWithFailover(() -> documentStoreHolder.get()[0].replace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(stream, fieldAsKey));
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, byte)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, byte inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, short)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, short inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, int)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, int inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, long)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, long inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, float)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, float inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, double)
     */
    public void increment(@NonNullable String _id, @NonNullable String field, double inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, BigDecimal)
     */
    public void increment(@NonNullable String _id, @NonNullable String field,
            @NonNullable BigDecimal inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, byte)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, byte inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, short)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, short inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, int)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, int inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, long)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, long inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, float)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, float inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, double)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field, double inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * Update operations are processed with NO failover strategy!
     *
     * @see DocumentStore#increment(String, String, BigDecimal)
     */
    public void increment(@NonNullable Value _id, @NonNullable String field,
            @NonNullable BigDecimal inc) throws StoreException {
        LOG.warn("Processing update on {} with no failover strategy!" +
                " Failover for update operations are not supported!", _id);
        documentStoreHolder.get()[0].increment(_id, field, inc);
    }

    /**
     * First of all we try to perform operation with the first DocumentStore.
     * If this operation takes a lot of time, we will perform operation with
     * second DocumentStore in parallel, and switch table for a  10/30/60/120s
     * according to how many times we have already changed tables previously.
     *
     * @param primaryTask   Operation with the primary DocumentStore.
     * @param secondaryTask Operation with the secondary DocumentStore.
     */
    private void processWithFailover(Runnable primaryTask, Runnable secondaryTask) {

        LOG.debug("Execute operation with Primary : {} , Secondary {}", this.getCurrentActiveTable(),
                this.getCurrentFailOverTable());

        // We create local executor because we need to have a possibility to shut down it.
        // When we make requests to the cluster, which unreachable,
        // this request will lock the thread in which it executed for some time,
        // and we don't want to wait for availability
        ExecutorService tableOperationExecutor = Executors.newFixedThreadPool(2);

        CompletableFuture<Void> primaryFuture = CompletableFuture.runAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table {} ", this.getCurrentActiveTable());
            return null;
        });

        try {
            primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.warn("Processing request to primary {} table too long, trying on secondary {} ",
                    this.getCurrentActiveTable(), this.getCurrentFailOverTable());

            // If timeOut tie exceeds, we make requests to the second cluster,
            // that will execute in parallel with request to primary cluster
            CompletableFuture<Void> secondaryFuture = CompletableFuture.runAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution with fail-over table {} ", this.getCurrentFailOverTable());
                return null;
            });

            // We combine primary and secondary CompletableFuture and execute this lambda,
            // if one of the futures is completed
            secondaryFuture.acceptEitherAsync(primaryFuture, s -> {
                // Check if the primary request is stuck, if so,
                // we shut down the executor without completion of a primary request
                if (primaryFuture.isCompletedExceptionally() || !primaryFuture.isDone()) {
                    tableOperationExecutor.shutdownNow();
                }
            });

            secondaryFuture.join();

            if (isClusterSwitched()) {
                // This is for determining time for switching
                int numberOfSwitch = counterForTableSwitching.getAndIncrement();
                // Switch to the secondary cluster adn prepare the system to switch back to primary
                // this method contain the logic to define when to switch back
                createAndExecuteTaskForSwitchingTable(getTimeOut(numberOfSwitch));
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Fail-over exception", e);
        }
    }

    /**
     * Work as <code>processWithFailover<code/> but with return value.
     *
     * @param primaryTask   Operation with the primary DocumentStore.
     * @param secondaryTask Operation with the secondary DocumentStore.
     * @param <R>           type of return value
     * @return value that defined in supplier
     */
    private <R> R processWithFailoverWithOutputData(Supplier<R> primaryTask, Supplier<R> secondaryTask) {

        LOG.debug("Execute operation with Primary : {} , Secondary {}", this.getCurrentActiveTable(),
                this.getCurrentFailOverTable());

        // We create local executor because we need to have a possibility to shut down it.
        // When we make requests to the cluster, which unreachable,
        // this request will lock the thread in which it executed for some time,
        // and we don't want to wait for availability
        CompletableFuture<R> primaryFuture = CompletableFuture.supplyAsync(primaryTask, tableOperationExecutor);

        primaryFuture.exceptionally(throwable -> {
            LOG.error("Problem while execution with primary table {} ", this.getCurrentActiveTable());
            throw new RetryPolicyException(throwable);
        });

        try {
            return primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {

            LOG.warn("Processing request to primary {} table too long, trying on secondary {} ",
                    this.getCurrentActiveTable(), this.getCurrentFailOverTable());

            // If timeOut tie exceeds, we make requests to the second cluster,
            // that will execute in parallel with request to primary cluster
            CompletableFuture<R> secondaryFuture = CompletableFuture.supplyAsync(secondaryTask, tableOperationExecutor);

            secondaryFuture.exceptionally(throwable -> {
                LOG.error("Problem while execution", throwable);
                throw new RetryPolicyException(throwable);
            });

            secondaryFuture.acceptEither(primaryFuture, r -> {
            });

            // We combine primary and secondary CompletableFuture and return result from the first that will succeed
            R result = secondaryFuture.join();

            if (isClusterSwitched()) {
                // This is for determining time for switching
                int numberOfSwitch = counterForTableSwitching.getAndIncrement();
                // Switch to the secondary cluster adn prepare the system to switch back to primary
                // this method contain the logic to define when to switch back
                createAndExecuteTaskForSwitchingTable(getTimeOut(numberOfSwitch));
            }
            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new RetryPolicyException(e);
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
        switched.set(true);
        // We create a task for scheduler, that will change a cluster back after 10s/1mn/2mn
        scheduler.schedule(
                () -> {
                    swapTableLinks();
                    switched.set(false); // indicates that we are not in "failovermode"
                    LOG.warn("Switching back to initial conf ( Primary : {} , Secondary {}   ) ", primaryTable, secondaryTable);
                }, timeForSwitchingTableBack, TimeUnit.MILLISECONDS);
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
     * Swap primary and secondary tables
     * <p>
     * When failing over to another table/cluster or when going back to origin/master cluster
     * we do not change the whole logic, but simply switch the primary/secondary tables
     */
    private void swapTableLinks() {
        while (true) {
            DocumentStore[] origin = documentStoreHolder.get();
            DocumentStore[] swapped = new DocumentStore[]{origin[1], origin[0]};
            if (documentStoreHolder.compareAndSet(origin, swapped)) {
                return;
            }
        }
    }


    /**
     * Get the path of the current active table
     *
     * @return the path of the current active table
     */
    private String getCurrentActiveTable() {
        if (isClusterSwitched()) {
            return primaryTable;
        } else {
            return secondaryTable;
        }
    }

    /**
     * Get the path of the current "fail over" table
     *
     * @return the path of the current "fail over" table
     */
    private String getCurrentFailOverTable() {
        if (isClusterSwitched()) {
            return secondaryTable; // if not switched the failover table is the "secondary" table
        } else {
            return primaryTable; // if switched the fail overt table is the "primary" table (from initial configuration)
        }
    }

    private boolean isClusterSwitched() {
        return !switched.get();
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
        documentStoreHolder.get()[0].close();
        documentStoreHolder.get()[1].close();
    }
}