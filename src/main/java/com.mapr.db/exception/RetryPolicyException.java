package com.mapr.db.exception;

/**
 * <p>Signals that an all retries is failed of some reasons. This
 * class is the general class of exceptions produced by failed or
 * interrupted retries operations. And this exception is unchecked.<p/>
 */
public class RetryPolicyException extends RuntimeException {

  /**
   * Constructs an {@code RetryPolicyException} with {@code null}
   * as its error detail message.
   */
  public RetryPolicyException() {
  }

  /**
   * Constructs an {@code RetryPolicyException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval
   *                by the {@link #getMessage()} method)
   */
  public RetryPolicyException(String message) {
    super(message);
  }

  /**
   * Constructs an {@code RetryPolicyException} with the specified cause and a
   * detail message of {@code (cause==null ? null : cause.toString())}
   * (which typically contains the class and detail message of {@code cause}).
   * This constructor is useful for Retry exceptions that are little more
   * than wrappers for other throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the
   *              {@link #getCause()} method).  (A null value is permitted,
   *              and indicates that the cause is nonexistent or unknown.)
   */
  public RetryPolicyException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs an {@code RetryPolicyException} with the specified detail message
   * and cause.
   * <p>
   * <p> Note that the detail message associated with {@code cause} is
   * <i>not</i> automatically incorporated into this exception's detail
   * message.
   *
   * @param message The detail message (which is saved for later retrieval
   *                by the {@link #getMessage()} method)
   * @param cause   The cause (which is saved for later retrieval by the
   *                {@link #getCause()} method).  (A null value is permitted,
   *                and indicates that the cause is nonexistent or unknown.)
   */
  public RetryPolicyException(String message, Throwable cause) {
    super(message, cause);
  }
}
