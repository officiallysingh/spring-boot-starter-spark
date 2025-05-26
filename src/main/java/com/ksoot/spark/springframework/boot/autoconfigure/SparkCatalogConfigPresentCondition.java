package com.ksoot.spark.springframework.boot.autoconfigure;

import static com.ksoot.spark.util.SparkConstants.SPARK_SQL_CATALOG_CONFIG_PREFIX;

import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SparkCatalogConfigPresentCondition implements Condition {

  @Override
  public boolean matches(
      final ConditionContext context, final @NotNull AnnotatedTypeMetadata metadata) {
    Environment environment = context.getEnvironment();
    if (environment instanceof ConfigurableEnvironment) {
      final List<PropertySource<?>> propertySources =
          ((ConfigurableEnvironment) environment).getPropertySources().stream().toList();
      final boolean sparkCatalogConfigPresent =
          propertySources.stream()
              .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
              .map(propertySource -> (EnumerablePropertySource) propertySource)
              .map(EnumerablePropertySource::getPropertyNames)
              .flatMap(Arrays::stream)
              .distinct()
              .anyMatch(key -> key.startsWith(SPARK_SQL_CATALOG_CONFIG_PREFIX));

      return sparkCatalogConfigPresent;
    } else {
      throw new IllegalStateException("Environment is not an instance of ConfigurableEnvironment");
    }
  }
}
