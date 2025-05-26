package com.ksoot.spark.util;

/**
 * A configuration related runtime exception.
 *
 * @since 1.0
 */
public class SparkConfigException extends RuntimeException {
  /** The serial version ID. */
  private static final long serialVersionUID = -7838702245512140996L;

  /** Constructs a new {@code ConfigurationRuntimeException} without specified detail message. */
  public SparkConfigException() {
    super();
  }

  /**
   * Constructs a new {@code ConfigurationRuntimeException} with specified detail message.
   *
   * @param message the error message
   */
  public SparkConfigException(final String message) {
    super(message);
  }

  /**
   * Constructs a new {@code ConfigurationRuntimeException} with specified detail message using
   * {@link String#format(String,Object...)}.
   *
   * @param message the error message
   * @param args arguments to the error message
   * @see String#format(String,Object...)
   */
  public SparkConfigException(final String message, final Object... args) {
    super(String.format(message, args));
  }

  /**
   * Constructs a new {@code ConfigurationRuntimeException} with specified nested {@code Throwable}.
   *
   * @param cause the exception or error that caused this exception to be thrown
   */
  public SparkConfigException(final Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new {@code ConfigurationRuntimeException} with specified detail message and nested
   * {@code Throwable}.
   *
   * @param message the error message
   * @param cause the exception or error that caused this exception to be thrown
   */
  public SparkConfigException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
