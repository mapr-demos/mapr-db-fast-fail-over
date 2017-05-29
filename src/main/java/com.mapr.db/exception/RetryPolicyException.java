package com.mapr.db.exception;

public class RetryPolicyException extends RuntimeException {

  public RetryPolicyException() {}

  public RetryPolicyException(String message) {
    super(message);
  }

  public RetryPolicyException (Throwable cause) {
    super (cause);
  }

  public RetryPolicyException (String message, Throwable cause) {
    super(message, cause);
  }
}
