package com.ksoot.spark.springframework.boot.autoconfigure;

import org.apache.spark.sql.SparkSession;

/**
 * Callback interface that can be implemented by beans wishing to further customize the {@link
 * SparkSession} through {@link SparkSession.Builder} retaining its default auto-configuration.
 */
@FunctionalInterface
public interface SparkSessionBuilderCustomizer {

  /**
   * Customize the SparkSession.Builder.
   *
   * @param sparkSessionBuilder the SparkSession.Builder to customize
   */
  void customize(final SparkSession.Builder sparkSessionBuilder);
}
