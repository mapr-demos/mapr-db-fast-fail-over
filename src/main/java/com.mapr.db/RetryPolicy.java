package com.mapr.db;

/**
 * The idea of the Retry Policy is to allow the developer
 * to configured at the table, operation level the behavior
 * of this operation:
 * <p>
 * DefaultRetryPolicy : standard MapR DB API behavior, wait until
 * the fail over is over, or time out based on TCP configuration.
 * <p>
 * The developer can create an instance of a RetryPolicy that will be
 * used/defined at different levels: (see below for detail requirements)
 * <ul>
 * <li>Overall Configuration</li>
 * <li>Table</li>
 * <li>Operation</li>
 * </ul>
 */
public class RetryPolicy {

    /**
     * the number of ms to wait before
     * the operation is send to the other
     * table/retried, default value to 100ms
     */
    private long timeout = 100;

    /**
     * the name of the table used for the fail over/replication,
     * for example   /apps/tables/user_data_fo
     */
    private String primaryTable;

    /**
     * the name of the table used for the fail over/replication,
     * for example   /apps/tables/user_data_fo
     */
    private String alternateTable;


    private RetryPolicy(Builder builder) {
        this.timeout = builder.timeout;
        this.primaryTable = builder.primaryTable;
        this.alternateTable = builder.alternateTable;
    }

    /**
     * Dummy constructor that needed for serialization to Json,
     * and deserialization to RetryPolicy object. Only for this purposes.
     */
    public RetryPolicy() {
        this.timeout = 0;
        this.primaryTable = null;
        this.alternateTable = null;
    }

    public long getTimeout() {
        return timeout;
    }

    public String getPrimaryTable() {
        return primaryTable;
    }

    public String getAlternateTable() {
        return alternateTable;
    }

    public static class Builder {

        private long timeout = 100;
        private String primaryTable;
        private String alternateTable;

        public Builder(String primaryTable, String alternateTable) {
            this.primaryTable = primaryTable;
            this.alternateTable = alternateTable;
        }

        /**
         * @param timeout the number of ms to wait before
         *                the operation is send to the other
         *                table/retried, default value to 100ms
         * @return Builder
         */
        public Builder setTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Terminal operation that will construct RetryPolicy object.
         *
         * @return RetryPolicy
         */
        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }
}
