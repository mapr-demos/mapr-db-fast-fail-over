package com.mapr.db.policy;

import lombok.*;
import org.codehaus.jackson.map.ObjectMapper;

// TODO why is there a single class in a package? The name of the package is even in the
// class name.
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
@Getter
@ToString
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class RetryPolicy {

    /**
     * the number of ms to wait before
     * the operation is send to the other
     * table/retried, default value to 100ms
     */
    @lombok.Builder.Default
    // TODO the failure strategy is wrong. There should be multiple timeouts and after a timeout, all operations should go to the secondary for a time
    private long timeout = 100;

    /**
     * the number of retries before raising
     * an operation exception, default value 3
     */
    @lombok.Builder.Default
    private int numberOfRetries = 3;

    // TODO this is wrong. The alternate should be kept open.
    /**
     * the name of the table used for the fail over/replication,
     * for example   /apps/tables/user_data_fo
     */
    private String alternateTable;

    /**
     * if the table name is not specified, it is possible to set a suffix,
     * for example _fo that will be used by all objects where this policy is set.
     */
    private String alternateSuffix;

    /**
     * Returns RetryPolicy object as Json representation.
     *
     * @return String with serialized to Json object.
     */
    public String toJson() {
        return convertToJson();
    }

    @SneakyThrows
    private String convertToJson() {
        return new ObjectMapper().writeValueAsString(this);
    }
}
