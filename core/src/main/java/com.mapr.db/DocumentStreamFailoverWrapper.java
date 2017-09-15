package com.mapr.db;

import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.exceptions.StoreException;

import java.util.Iterator;

public class DocumentStreamFailoverWrapper implements DocumentStream {

    private DocumentStream stream;

    DocumentStreamFailoverWrapper(DocumentStream stream) {
        this.stream = stream;
    }

    /**
     * {@inheritDoc}
     *
     * @throws EnhancedJSONTable.FailoverException if we catch  {@code StoreException} from primary table and
     *                                             cannot provide failover for the request
     */
    @Override
    public void streamTo(DocumentListener listener) {
        try {
            this.stream.streamTo(listener);
        } catch (StoreException se) {
            throw new EnhancedJSONTable.FailoverException("Cannot provide failover for this operation. " +
                    "Try to check your primary cluster, and try again.", se);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws EnhancedJSONTable.FailoverException if we catch  {@code StoreException} from primary table and
     *                                             cannot provide failover for the request
     */
    @Override
    public Iterator<Document> iterator() {
        try {
            return this.stream.iterator();
        } catch (StoreException se) {
            throw new EnhancedJSONTable.FailoverException("Cannot provide failover for this operation. " +
                    "Try to check your primary cluster, and try again.", se);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws EnhancedJSONTable.FailoverException if we catch  {@code StoreException} from primary table and
     *                                             cannot provide failover for the request
     */
    @Override
    public Iterable<DocumentReader> documentReaders() {
        try {
            return this.stream.documentReaders();
        } catch (StoreException se) {
            throw new EnhancedJSONTable.FailoverException("Cannot provide failover for this operation. " +
                    "Try to check your primary cluster, and try again.", se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws OjaiException {
        this.stream.close();
    }
}
