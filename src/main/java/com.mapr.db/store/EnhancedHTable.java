package com.mapr.db.store;

import com.mapr.db.exception.RetryPolicyException;
import com.mapr.db.policy.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mapr.db.Util.getTable;

@Slf4j
public class EnhancedHTable extends HTable {

    private final RetryPolicy policy;

    /**
     * Creates an object to access a HBase table.
     *
     * @param configuration Configuration object to use.
     * @param tableName     Name of the table.
     * @throws IOException if a remote or network exception occurs
     */
    public EnhancedHTable(Configuration configuration, String tableName) throws IOException {
        super(configuration, tableName);
        this.policy = getPolicyFrom(configuration);
    }

    /**
     * Creates an object to access a HBase table.
     *
     * @param configuration Configuration object to use.
     * @param table         Name of the table.
     * @param retryPolicy   Data for configuration retry policy.
     * @throws IOException if a remote or network exception occurs
     */
    public EnhancedHTable(Configuration configuration, String table, RetryPolicy retryPolicy) throws IOException {
        super(configuration, table);
        this.policy = retryPolicy;
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public Result append(Append append) {
        return append(append, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param append      Performs Append operations on a single row.
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return Single row result of a query.<p>
     */
    public Result append(Append append, RetryPolicy retryPolicy) {
        CompletableFuture<Result> completeFuture =
                CompletableFuture.supplyAsync(() -> {
                    int numberOfRetries = retryPolicy.getNumberOfRetries();
                    while (numberOfRetries != 0) {
                        try {
                            return super.append(append);
                        } catch (IOException ignored) {
                            numberOfRetries--;
                        }
                    }
                    throw new RuntimeException();
                });
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new RetryPolicyException();
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row,
            byte[] family, byte[] qualifier, byte[] value,
            Delete delete) {
        return checkAndDelete(row, family, qualifier, value, delete, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return true if operation successful, false if some trouble was occurred.
     */
    public boolean checkAndDelete(byte[] row,
            byte[] family, byte[] qualifier, byte[] value,
            Delete delete, RetryPolicy retryPolicy) {
        Supplier<Boolean> supplier = () -> {
            try {
                return super.checkAndDelete(row, family, qualifier, value, delete);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Boolean> completableFuture = processRequestWithRetries(supplier, retryPolicy);
        try {
            return completableFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Supplier<Boolean> putTask = () -> {
                try {
                    return getTable(tableName).checkAndDelete(row, family, qualifier, value, delete);
                } catch (IOException e1) {
                    throw new RetryPolicyException();
                }
            };
            return performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean checkAndDelete(byte[] row, byte[] family,
            byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value,
            Delete delete) {
        return checkAndDelete(row, family, qualifier, compareOp, value, delete, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return true if operation successful, false if some trouble was occurred.
     */
    public boolean checkAndDelete(byte[] row, byte[] family,
            byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value,
            Delete delete, RetryPolicy retryPolicy) {
        Supplier<Boolean> supplier = () -> {
            try {
                return super.checkAndDelete(row, family, qualifier, compareOp, value, delete);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Boolean> completeFuture = processRequestWithRetries(supplier, retryPolicy);
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Supplier<Boolean> putTask = () -> {
                try {
                    return getTable(tableName).checkAndDelete(row, family, qualifier, compareOp, value, delete);
                } catch (IOException e1) {
                    throw new RetryPolicyException();
                }
            };
            return performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, byte[] value,
            RowMutations rm) {
        return checkAndMutate(row, family, qualifier, compareOp, value, rm, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return
     */
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, byte[] value,
            RowMutations rm, RetryPolicy retryPolicy) {
        Supplier<Boolean> supplier = () -> {
            try {
                return super.checkAndMutate(row, family, qualifier, compareOp, value, rm);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Boolean> completeFuture
                = processRequestWithRetries(supplier, retryPolicy);
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Supplier<Boolean> putTask = () -> {
                try {
                    return getTable(tableName).checkAndMutate(row, family, qualifier, compareOp, value, rm);
                } catch (IOException e1) {
                    throw new RetryPolicyException();
                }
            };
            return performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean checkAndPut(byte[] row,
            byte[] family, byte[] qualifier, byte[] value,
            Put put) {
        return checkAndPut(row, family, qualifier, value, put, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return
     */
    public boolean checkAndPut(byte[] row,
            byte[] family, byte[] qualifier, byte[] value,
            Put put, RetryPolicy retryPolicy) {
        Supplier<Boolean> supplier = () -> {
            try {
                return super.checkAndPut(row, family, qualifier, value, put);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Boolean> completeFuture
                = processRequestWithRetries(supplier, retryPolicy);
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Supplier<Boolean> putTask = () -> {
                try {
                    return getTable(tableName).checkAndPut(row, family, qualifier, value, put);
                } catch (IOException e1) {
                    throw new RetryPolicyException();
                }
            };
            return performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public void delete(Delete delete) {
        delete(delete, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     */
    public void delete(Delete delete, RetryPolicy retryPolicy) {
        List<Delete> deletes = new LinkedList<>();
        deletes.add(delete);
        this.delete(deletes, retryPolicy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public void delete(List<Delete> deletes) {
        delete(deletes, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     */
    public void delete(List<Delete> deletes, RetryPolicy retryPolicy) {
        Consumer<Void> consumer = s -> {
            try {
                super.delete(deletes);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Void> completeFuture = processRequestWithRetries(consumer, retryPolicy);
        try {
            completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new RetryPolicyException();
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean exists(Get get) {
        return exists(get, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param get
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return
     */
    public boolean exists(Get get, RetryPolicy retryPolicy) {
        Supplier<Boolean> supplier = () -> {
            try {
                return super.exists(get);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Boolean> completeFuture = processRequestWithRetries(supplier, retryPolicy);
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Supplier<Boolean> putTask = () -> {
                try {
                    return getTable(tableName).exists(get);
                } catch (IOException e1) {
                    throw new RetryPolicyException();
                }
            };
            return performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public boolean[] existsAll(List<Get> gets) throws IOException {
        return existsAll(gets, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     * @return
     */
    public boolean[] existsAll(List<Get> gets, RetryPolicy retryPolicy) throws IOException {
        CompletableFuture<boolean[]> completeFuture =
                CompletableFuture.supplyAsync(() -> {
                    int numberOfRetries = retryPolicy.getNumberOfRetries();
                    while (numberOfRetries != 0) {
                        try {
                            return super.existsAll(gets);
                        } catch (IOException ignored) {
                            numberOfRetries--;
                        }
                    }
                    throw new RuntimeException();
                });
        try {
            return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            if (isAlternateTableExist(policy)) {
                return getTable(policy.getAlternateTable()).existsAll(gets);
            }
            throw new RetryPolicyException();
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public void mutateRow(RowMutations rm) {
        mutateRow(rm, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     */
    public void mutateRow(RowMutations rm, RetryPolicy retryPolicy) {
        Consumer<Void> consumer = s -> {
            try {
                super.mutateRow(rm);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Void> completeFuture = processRequestWithRetries(consumer, retryPolicy);
        try {
            completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Consumer<Void> putTask = s -> {
                try {
                    getTable(tableName).mutateRow(rm);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            };
            performOperationWithAlternativeTable(putTask);
        }
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     */
    @Override
    public void put(Put put) {
        put(put, policy);
    }

    /**
     * For more info about standard behaviour see {@inheritDoc}
     *
     * @param retryPolicy Data for configuration retry policy for this operation.
     */
    public void put(Put put, RetryPolicy retryPolicy) {
        Consumer<Void> consumer = s -> {
            try {
                super.put(put);
            } catch (IOException e) {
                throw new RetryPolicyException();
            }
        };
        CompletableFuture<Void> completeFuture = processRequestWithRetries(consumer, retryPolicy);
        try {
            completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            String tableName = getAlternativeTableName();
            Consumer<Void> putTask = s -> {
                try {
                    getTable(tableName).put(put);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            };
            performOperationWithAlternativeTable(putTask);
        }
    }

    private RetryPolicy getPolicyFrom(Configuration configuration) throws IOException {
        return new ObjectMapper().readValue(configuration.get("mapr.db.retry.policy"), RetryPolicy.class);
    }

    private boolean isAlternateTableExist(RetryPolicy policy) {
        return policy.getAlternateTable() != null;
    }

    private void performOperationWithAlternativeTable(Consumer<Void> consumer) {
        printInfoAboutAlternativeTable();
        consumer.accept(null);
        log.info("Success!");
    }

    private Boolean performOperationWithAlternativeTable(Supplier<Boolean> supplier) {
        printInfoAboutAlternativeTable();
        return supplier.get();
    }

    private void printInfoAboutAlternativeTable() {
        log.info("Failed operation with main table, path - {}", new String(getTableName()));
        log.info("Trying perform this operation with alternative table");
        log.info("path - {}", getAlternativeTableName());
    }

    private String getAlternativeTableName() {
        String tableName;
        if (isAlternateTableExist(policy)) {
            tableName = policy.getAlternateTable();
        } else {
            tableName = new String(getTableName()) + "_alternate";
        }
        return tableName;
    }

    /**
     * Process the request provided by consumer using provided RetryPolicy
     *
     * @param consumer    request
     * @param retryPolicy retry policy
     * @return result of action
     */
    private CompletableFuture<Void> processRequestWithRetries(Consumer<Void> consumer, RetryPolicy retryPolicy) {
        return CompletableFuture.supplyAsync(() -> {
            int numberOfRetries = retryPolicy.getNumberOfRetries();
            while (numberOfRetries != 0) {
                try {
                    consumer.accept(null);
                    return null;
                } catch (RetryPolicyException rpe) {
                    numberOfRetries--;
                }
            }
            throw new RuntimeException();
        });
    }

    /**
     * Process the request provided by supplier using provided RetryPolicy
     *
     * @param supplier    request
     * @param retryPolicy retry policy
     * @return result of action
     */
    private CompletableFuture<Boolean> processRequestWithRetries(Supplier<Boolean> supplier, RetryPolicy retryPolicy) {
        return CompletableFuture.supplyAsync(() -> {
            int numberOfRetries = retryPolicy.getNumberOfRetries();
            while (numberOfRetries != 0) {
                try {
                    return supplier.get();
                } catch (RetryPolicyException rpe) {
                    numberOfRetries--;
                }
            }
            throw new RuntimeException();
        });
    }
}
