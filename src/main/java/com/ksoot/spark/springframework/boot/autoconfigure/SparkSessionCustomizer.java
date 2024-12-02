package com.ksoot.spark.springframework.boot.autoconfigure;

import org.apache.spark.sql.SparkSession;

/**
 * Callback interface that can be implemented by beans wishing to further customize the {@link
 * SparkSession} retaining its default auto-configuration.
 */
@FunctionalInterface
public interface SparkSessionCustomizer {

  /**
   * Customize the SparkSession. Like register UDFs etc.
   *
   * @param sparkSession the SparkSession to customize
   */
  void customize(final SparkSession sparkSession);
}
