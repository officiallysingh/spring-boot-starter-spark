package com.ksoot.spark.springframework.boot.autoconfigure;

import static com.ksoot.spark.util.SparkConstants.SPARK_PREFIX;
import static com.ksoot.spark.util.SparkConstants.SPARK_PROPERTIES_BEAN_NAME;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.*;

@AutoConfiguration
public class SparkAutoConfiguration {

  @ConditionalOnClass(SparkSession.class)
  @ConditionalOnMissingBean(SparkSession.class)
  static class SparkSessionConfiguration {

    @Bean(destroyMethod = "stop")
    SparkSession sparkSession(
        final SparkSession.Builder sparkSessionBuilder,
        final List<SparkSessionCustomizer> customizers) {
      final SparkSession sparkSession = sparkSessionBuilder.getOrCreate();
      for (SparkSessionCustomizer customizer : customizers) {
        customizer.customize(sparkSession);
      }
      return sparkSession;
    }
  }

  @ConditionalOnClass(SparkSession.Builder.class)
  @ConditionalOnMissingBean(SparkSession.Builder.class)
  static class SparkSessionBuilderConfiguration {

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    SparkSession.Builder sparkSessionBuilder(
        final SparkConf sparkConf, final List<SparkSessionBuilderCustomizer> customizers) {
      SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
      for (SparkSessionBuilderCustomizer customizer : customizers) {
        customizer.customize(builder);
      }
      return builder;
    }
  }

  @ConditionalOnClass(SparkConf.class)
  @ConditionalOnMissingBean(SparkConf.class)
  static class SparkConfConfiguration {

    @Bean
    SparkConf sparkConf(@Qualifier(SPARK_PROPERTIES_BEAN_NAME) final Properties sparkProperties) {
      final SparkConf sparkConf = new SparkConf();
      sparkProperties.forEach((key, value) -> sparkConf.set(key.toString(), value.toString()));
      return sparkConf;
    }
  }

  @ConditionalOnMissingBean(name = SPARK_PROPERTIES_BEAN_NAME)
  static class SparkPropertiesConfiguration {

    @Bean
    Properties sparkProperties(final Environment environment) {
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
                .filter(key -> key.startsWith(SPARK_PREFIX))
                .collect(Collectors.toList());

        return sparkPropertyNames.stream()
            .collect(
                Properties::new,
                (props, key) -> props.put(key, environment.getProperty(key)),
                Properties::putAll);
      } else {
        return new Properties();
      }
    }
  }

  @ConditionalOnMissingBean(Configuration.class)
  static class SparkHadoopConfiguration {

    @Bean
    Configuration hadoopConfiguration(final SparkSession sparkSession) {
      //      Configuration hadoopConfiguration = new Configuration();
      //
      // hadoopConfiguration.set(org.apache.iceberg.hadoop.ConfigProperties.ENGINE_HIVE_ENABLED,
      // "true");
      return sparkSession.sparkContext().hadoopConfiguration();
    }
  }
}
