package com.mapr.db.policy;

import lombok.Getter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

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

  public RetryPolicy() {
    this.timeout = 0;
    this.numOfRetries = 0;
    this.alternateTable = null;
    this.alternateSuffix = null;
  }

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

    private long timeout;
    private int numOfRetries;
    private String alternateTable;
    private String alternateSuffix;

    public Builder() {
    }

    public Builder setTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder setNumOfReties(int numOfReties) {
      this.numOfRetries = numOfReties;
      return this;
    }

    public Builder setAlternateTable(String alternateTable) {
      this.alternateTable = alternateTable;
      return this;
    }

    public Builder setAlternateSuffix(String alternateSuffix) {
      this.alternateSuffix = alternateSuffix;
      return this;
    }

    public RetryPolicy build() {
      return new RetryPolicy(this);
    }
  }
}
