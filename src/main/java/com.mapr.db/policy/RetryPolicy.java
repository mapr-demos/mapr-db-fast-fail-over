package com.mapr.db.policy;

import lombok.Getter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

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
public class RetryPolicy {

  private final long timeout;
  private final int numOfRetries;
  private final String alternateTable;
  private final String alternateSuffix;

  private RetryPolicy(Builder builder) {
    this.timeout = builder.timeout;
    this.numOfRetries = builder.numOfRetries;
    this.alternateTable = builder.alternateTable;
    this.alternateSuffix = builder.alternateSuffix;
  }

  /**
   * Dummy constructor that needed for serialization to Json,
   * and deserialization to RetryPolicy object. Only for this purposes.
   */
  public RetryPolicy() {
    this.timeout = 0;
    this.numOfRetries = 0;
    this.alternateTable = null;
    this.alternateSuffix = null;
  }

  /**
   * Serialize RetryPolicy object to Json representation.
   *
   * @return String with serialized to Json object.
   */
  public String toJson() {
    return convertToJson();
  }

  private String convertToJson() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (IOException e) {
      return "{}";
    }
  }

  @Override
  public String toString() {
    return "RetryPolicy{" +
        "timeout=" + timeout +
        ", numOfRetries=" + numOfRetries +
        ", alternateTable='" + alternateTable + '\'' +
        ", alternateSuffix='" + alternateSuffix + '\'' +
        '}';
  }

  public static class Builder {

    private long timeout = 100;
    private int numOfRetries = 3;
    private String alternateTable;
    private String alternateSuffix;

    public Builder() {
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
     * @param numOfReties the number of retries before raising
     *                    an operation exception, default value 3
     * @return Builder
     */
    public Builder setNumOfReties(int numOfReties) {
      this.numOfRetries = numOfReties;
      return this;
    }

    /**
     * @param alternateTable the name of the table used for the fail over/replication,
     *                       for example   /apps/tables/user_data_fo
     * @return Builder
     */
    public Builder setAlternateTable(String alternateTable) {
      this.alternateTable = alternateTable;
      return this;
    }

    /**
     * Not implemented yet.
     *
     * @param alternateSuffix if the table name is not specified, it is possible to set a suffix,
     *                        for example _fo that will be used by all objects where this policy is set.
     * @return Builder
     */
    public Builder setAlternateSuffix(String alternateSuffix) {
      this.alternateSuffix = alternateSuffix;
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
