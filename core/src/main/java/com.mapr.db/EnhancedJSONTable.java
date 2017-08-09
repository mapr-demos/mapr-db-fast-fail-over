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

    private long timeOut;

    private AtomicReference<DocumentStore[]> documentStoreHolder;

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
        DocumentStore primary = getJsonTable(primaryTable);
        DocumentStore secondary = getJsonTable(secondaryTable);
        this.timeOut = timeOut;

        this.documentStoreHolder = new AtomicReference<>(new DocumentStore[]{primary, secondary});
    }

    /**
     * See OJAI findById
     */
    public Document findById(String _id) {
        try {
            return tryFindById(_id);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(Value _id) {
        try {
            return tryFindById(_id);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }


    /**
     * See OJAI findById
     */
    public Document findById(String _id, String... fieldPaths) {
        try {
            return tryFindById(_id, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(String _id, QueryCondition condition) {
        try {
            return tryFindById(_id, condition);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(Value _id, QueryCondition condition) {
        try {
            return tryFindById(_id, condition);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(String _id, QueryCondition condition, String... fieldPaths) {
        try {
            return tryFindById(_id, condition, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(String _id, QueryCondition condition, FieldPath... fieldPaths) {
        try {
            return tryFindById(_id, condition, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(Value _id, QueryCondition condition, String... fieldPaths) {
        try {
            return tryFindById(_id, condition, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findById
     */
    public Document findById(Value _id, QueryCondition condition, FieldPath... fieldPaths) {
        try {
            return tryFindById(_id, condition, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI find
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
     * See OJAI findQuery
     */
    public DocumentStream findQuery(Query query) {
        try {
            return tryFindQuery(query);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI findQuery
     */
    public DocumentStream findQuery(String queryJSON) {
        try {
            return tryFindQuery(queryJSON);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI find
     */
    public DocumentStream find(String... fieldPaths) {
        try {
            return tryFind(fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI find
     */
    public DocumentStream find(FieldPath... fieldPaths) {
        try {
            return tryFind(fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI find
     */
    public DocumentStream find(QueryCondition condition) {
        try {
            return tryFind(condition);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI find
     */
    public DocumentStream find(QueryCondition condition, String... fieldPaths) {
        try {
            return tryFind(condition, fieldPaths);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(Document document) {
        try {
            tryInsertOrReplace(document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(String _id, Document document) {
        try {
            tryInsertOrReplace(_id, document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(Value _id, Document document) {
        try {
            tryInsertOrReplace(_id, document);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(Document document, FieldPath fieldAsKey) {
        try {
            tryInsertOrReplace(document, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(Document document, String fieldAsKey) {
        try {
            tryInsertOrReplace(document, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(DocumentStream stream) {
        try {
            tryInsertOrReplace(stream);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) {
        try {
            tryInsertOrReplace(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insertOrReplace
     */
    public void insertOrReplace(DocumentStream stream, String fieldAsKey) {
        try {
            tryInsertOrReplace(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI update
     */
    public void update(String _id, DocumentMutation mutation) {
        try {
            tryUpdate(_id, mutation);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI update
     */
    public void update(Value _id, DocumentMutation mutation) {
        try {
            tryUpdate(_id, mutation);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(String _id) {
        try {
            tryDelete(_id);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(Value _id) {
        try {
            tryDelete(_id);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(Document doc) {
        try {
            tryDelete(doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(Document doc, FieldPath fieldAsKey) {
        try {
            tryDelete(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(Document doc, String fieldAsKey) {
        try {
            tryDelete(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(DocumentStream stream) {
        try {
            tryDelete(stream);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(DocumentStream stream, FieldPath fieldAsKey) {
        try {
            tryDelete(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI delete
     */
    public void delete(DocumentStream stream, String fieldAsKey) {
        try {
            tryDelete(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(String _id, Document doc) {
        try {
            tryInsert(_id, doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(Value _id, Document doc) {
        try {
            tryInsert(_id, doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
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
     * See OJAI insert
     */
    public void insert(Document doc, FieldPath fieldAsKey) {
        try {
            tryInsert(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(Document doc, String fieldAsKey) {
        try {
            tryInsert(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(DocumentStream stream) {
        try {
            tryInsert(stream);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(DocumentStream stream, FieldPath fieldAsKey) {
        try {
            tryInsert(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI insert
     */
    public void insert(DocumentStream stream, String fieldAsKey) {
        try {
            tryInsert(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(String _id, Document doc) {
        try {
            tryReplace(_id, doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(Value _id, Document doc) {
        try {
            tryReplace(_id, doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(Document doc) {
        try {
            tryReplace(doc);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(Document doc, FieldPath fieldAsKey) {
        try {
            tryReplace(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(Document doc, String fieldAsKey) {
        try {
            tryReplace(doc, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(DocumentStream stream) {
        try {
            tryReplace(stream);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(DocumentStream stream, FieldPath fieldAsKey) {
        try {
            tryReplace(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * See OJAI replace
     */
    public void replace(DocumentStream stream, String fieldAsKey) {
        try {
            tryReplace(stream, fieldAsKey);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("Problem while execution ", e);
            throw new RetryPolicyException(e);
        }
    }

    /**
     * This private methods:
     * - call find on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     *
     * @return the document coming from primary or secondary table
     */
    private Document tryFindById(String _id) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id),
                () -> documentStoreHolder.get()[1].findById(_id));
    }

    private Document tryFindById(Value _id) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id),
                () -> documentStoreHolder.get()[1].findById(_id));
    }

    private Document tryFindById(String _id, String... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, fieldPaths));
    }


    private Document tryFindById(String _id, QueryCondition condition) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition),
                () -> documentStoreHolder.get()[1].findById(_id, condition));
    }


    private Document tryFindById(Value _id, QueryCondition condition) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition),
                () -> documentStoreHolder.get()[1].findById(_id, condition));
    }


    private Document tryFindById(String _id, QueryCondition condition, String... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }


    private Document tryFindById(String _id, QueryCondition condition, FieldPath... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }


    private Document tryFindById(Value _id, QueryCondition condition, String... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }


    private Document tryFindById(Value _id, QueryCondition condition, FieldPath... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findById(_id, condition, fieldPaths),
                () -> documentStoreHolder.get()[1].findById(_id, condition, fieldPaths));
    }


    private DocumentStream tryFind() throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].find(),
                () -> documentStoreHolder.get()[1].find());
    }


    private DocumentStream tryFindQuery(Query query) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findQuery(query),
                () -> documentStoreHolder.get()[1].findQuery(query));
    }


    private DocumentStream tryFindQuery(String queryJSON) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].findQuery(queryJSON),
                () -> documentStoreHolder.get()[1].findQuery(queryJSON));
    }


    private DocumentStream tryFind(String... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].find(fieldPaths),
                () -> documentStoreHolder.get()[1].find(fieldPaths));
    }


    private DocumentStream tryFind(FieldPath... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].find(fieldPaths),
                () -> documentStoreHolder.get()[1].find(fieldPaths));
    }

    private DocumentStream tryFind(QueryCondition condition) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].find(condition),
                () -> documentStoreHolder.get()[1].find(condition));
    }


    private DocumentStream tryFind(QueryCondition condition, String... fieldPaths) throws IOException, InterruptedException, ExecutionException {
        return performRetryLogicWithOutputData(() -> documentStoreHolder.get()[0].find(condition, fieldPaths),
                () -> documentStoreHolder.get()[1].find(condition, fieldPaths));
    }

    /**
     * This private methods:
     * - call insertOrReplace on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     */
    private void tryInsertOrReplace(Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(document),
                () -> documentStoreHolder.get()[1].insertOrReplace(document));
    }

    private void tryInsertOrReplace(String _id, Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(_id, document),
                () -> documentStoreHolder.get()[1].insertOrReplace(_id, document));
    }

    private void tryInsertOrReplace(Value _id, Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(_id, document),
                () -> documentStoreHolder.get()[1].insertOrReplace(_id, document));
    }

    private void tryInsertOrReplace(Document document, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(document, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(document, fieldAsKey));
    }

    private void tryInsertOrReplace(Document document, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(document, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(document, fieldAsKey));
    }

    private void tryInsertOrReplace(DocumentStream stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(stream),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream));
    }

    private void tryInsertOrReplace(DocumentStream stream, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream, fieldAsKey));
    }

    private void tryInsertOrReplace(DocumentStream stream, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insertOrReplace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insertOrReplace(stream, fieldAsKey));
    }

    /**
     * This private methods:
     * - call update on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     */
    private void tryUpdate(String _id, DocumentMutation mutation) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].update(_id, mutation),
                () -> documentStoreHolder.get()[1].update(_id, mutation));
    }

    private void tryUpdate(Value _id, DocumentMutation mutation) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].update(_id, mutation),
                () -> documentStoreHolder.get()[1].update(_id, mutation));
    }

    /**
     * This private methods:
     * - call delete on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     */
    private void tryDelete(String _id) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(_id),
                () -> documentStoreHolder.get()[1].delete(_id));
    }

    private void tryDelete(Value _id) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(_id),
                () -> documentStoreHolder.get()[1].delete(_id));
    }

    private void tryDelete(Document doc) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(doc),
                () -> documentStoreHolder.get()[1].delete(doc));
    }

    private void tryDelete(Document doc, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(doc, fieldAsKey));
    }

    private void tryDelete(Document doc, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(doc, fieldAsKey));
    }

    private void tryDelete(DocumentStream stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(stream),
                () -> documentStoreHolder.get()[1].delete(stream));
    }

    private void tryDelete(DocumentStream stream, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(stream, fieldAsKey));
    }

    private void tryDelete(DocumentStream stream, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].delete(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].delete(stream, fieldAsKey));
    }

    /**
     * This private methods:
     * - call insert on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     */
    private void tryInsert(String _id, Document stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(_id, stream),
                () -> documentStoreHolder.get()[1].insert(_id, stream));
    }

    private void tryInsert(Value _id, Document stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(_id, stream),
                () -> documentStoreHolder.get()[1].insert(_id, stream));
    }

    private void tryInsert(Document document) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(document),
                () -> documentStoreHolder.get()[1].insert(document));
    }

    private void tryInsert(Document doc, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(doc, fieldAsKey));
    }

    private void tryInsert(Document doc, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(doc, fieldAsKey));
    }

    private void tryInsert(DocumentStream stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(stream),
                () -> documentStoreHolder.get()[1].insert(stream));
    }

    private void tryInsert(DocumentStream stream, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(stream, fieldAsKey));
    }

    private void tryInsert(DocumentStream stream, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].insert(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].insert(stream, fieldAsKey));
    }

    /**
     * This private methods:
     * - call replace on primary table and secondary table if issue
     * using the <code>performRetryLogicWithOutputData</code> method.
     * <p>
     * In case of fail over this method stays on the failover table.
     */
    private void tryReplace(String _id, Document doc) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(_id, doc),
                () -> documentStoreHolder.get()[1].replace(_id, doc));
    }

    private void tryReplace(Value _id, Document doc) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(_id, doc),
                () -> documentStoreHolder.get()[1].replace(_id, doc));
    }

    private void tryReplace(Document doc) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(doc),
                () -> documentStoreHolder.get()[1].replace(doc));
    }

    private void tryReplace(Document doc, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(doc, fieldAsKey));
    }

    private void tryReplace(Document doc, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(doc, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(doc, fieldAsKey));
    }

    private void tryReplace(DocumentStream stream) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(stream),
                () -> documentStoreHolder.get()[1].replace(stream));
    }

    private void tryReplace(DocumentStream stream, FieldPath fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(stream, fieldAsKey));
    }

    private void tryReplace(DocumentStream stream, String fieldAsKey) throws IOException, InterruptedException, ExecutionException {
        performRetryLogic(() -> documentStoreHolder.get()[0].replace(stream, fieldAsKey),
                () -> documentStoreHolder.get()[1].replace(stream, fieldAsKey));
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

    /**
     * Work as <code>performRetryLogic<code/> but with return value.
     *
     * @param primaryTask   Operation with the primary DocumentStore.
     * @param secondaryTask Operation with the secondary DocumentStore.
     * @param <R>           type of return value
     * @return value that defined in supplier
     */
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
        long minute = 60000;
        switch (numberOfSwitch) {
            case 0:
                return minute / 6;
            case 1:
                return minute / 2;
            case 2:
                return minute;
            default:
                return 2 * minute;
        }
    }

    /**
     * Swap primary and secondary tables
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