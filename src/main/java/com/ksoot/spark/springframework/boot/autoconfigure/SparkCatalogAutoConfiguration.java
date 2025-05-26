package com.ksoot.spark.springframework.boot.autoconfigure;

import static com.ksoot.spark.util.SparkConstants.SPARK_SQL_CATALOG_CONFIG_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.ksoot.spark.util.SparkConfigException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

@Slf4j
@AutoConfiguration
@AutoConfigureAfter(SparkAutoConfiguration.class)
@Conditional(SparkCatalogConfigPresentCondition.class)
public class SparkCatalogAutoConfiguration {

  @ConditionalOnMissingBean(CatalogProperties.class)
  static class SparkCatalogPropertiesConfiguration {

    @Bean
    CatalogProperties catalogProperties(final Environment environment) {
      if (environment instanceof ConfigurableEnvironment) {
        final List<PropertySource<?>> propertySources =
            ((ConfigurableEnvironment) environment)
                .getPropertySources().stream().collect(Collectors.toList());
        final List<String> sparkPropertyNames =
            propertySources.stream()
                .filter(propertySource -> propertySource instanceof EnumerablePropertySource)
                .map(propertySource -> (EnumerablePropertySource) propertySource)
                .map(EnumerablePropertySource::getPropertyNames)
                .flatMap(Arrays::stream)
                .distinct()
                .filter(key -> key.startsWith(SPARK_SQL_CATALOG_CONFIG_PREFIX))
                .collect(Collectors.toList());

        Map<String, Object> catalogProps =
            sparkPropertyNames.stream()
                .collect(
                    HashMap::new,
                    (map, key) ->
                        map.put(
                            key.replace(SPARK_SQL_CATALOG_CONFIG_PREFIX, ""),
                            environment.getProperty(key)),
                    HashMap::putAll);
        final String catalogName =
            catalogProps.keySet().stream().filter(prop -> !prop.contains(".")).findFirst().get();
        catalogProps.remove(catalogName);
        catalogProps =
            catalogProps.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        entry -> entry.getKey().replace(catalogName + ".", ""),
                        Map.Entry::getValue));
        return CatalogProperties.of(catalogName, catalogProps);
      } else {
        throw new IllegalStateException(
            "Environment is not an instance of ConfigurableEnvironment");
      }
    }
  }

  @SneakyThrows
  @ConditionalOnMissingBean(Catalog.class)
  @Bean
  Catalog catalog(final SparkSession sparkSession, final CatalogProperties catalogProperties) {
    //    CatalogPlugin catalogPlugin =
    // sparkSession.sessionState().catalogManager().currentCatalog();
    CatalogPlugin catalogPlugin =
        sparkSession.sessionState().catalogManager().catalog(catalogProperties.getName());
    if (catalogPlugin instanceof SparkCatalog catalog) {
      final String[] namespace = new String[] {catalogProperties.getNamespace()};
      if (!catalog.namespaceExists(namespace)) {
        catalog.createNamespace(namespace, ImmutableMap.of());
      }
      return catalog.icebergCatalog();
    } else {
      log.error(
          "The catalog is not an instance of SparkCatalog, but of type: {}",
          catalogPlugin.getClass());
      throw new SparkConfigException("The catalog is not an instance of SparkCatalog");
    }
  }
}
