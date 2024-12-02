# Introduction
Managing dependencies is a crucial part of any complex project. Handling this manually can be tedious and time-consuming, leaving less room to focus on other essential aspects of development.

This is where [**Spring Boot starters**](https://github.com/spring-projects/spring-boot/blob/main/spring-boot-project/spring-boot-starters/README.adoc) come into play. These are pre-configured dependency descriptors designed to simplify dependency management. By including a starter POM in your project, you can effortlessly bring in all the necessary Spring and related technologies, saving you from the hassle of searching through documentation or copying multiple dependency definitions.

Spring Boot offers starters for popular technologies to streamline your development process. Though starter for Spark is not available yet.

# Spring Boot Starter Spark
The Spring Boot Starter for Spark is a set of convenient dependency descriptors that you can include in your Spring boot application  
to have all required Spark dependencies and [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) bean auto-configured with spark configurations support in spring boot `yml` or `properties` file.

## Dependency versions
It specifies the following versions:
* Java 17
* Spring Boot 3.4.0
* Spark 3.5.3
* Scala 2.12.18

## Features
* Bundles spark dependencies compatible with Spring boot 3+.   
* Provides auto-configured [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) bean which can be customized in any manner.
* Exposes all Spark configurations as Spring boot environment properties.
* Enables auto-completion assistance for Spark configuration properties in Spring boot `yml` and `properties` files in IDEs such as IntelliJ, Eclipse etc. Find details at [**additional-spring-configuration-metadata.json**](src/main/resources/META-INF/additional-spring-configuration-metadata.json)

## Usage
Add the following dependency to your `pom.xml`:

Define the following properties in `pom.xml`:

```xml
<properties>
    <java.version>17</java.version>
    <spring-boot.version>3.4.0</spring-boot.version>

    <spring-boot-starter-spark.version>0.0.1-SNAPSHOT</spring-boot-starter-spark.version>
    <!-- The Following two versions must be specified otherwise you will get exception java.lang.ClassNotFoundException: javax.servlet.http.HttpServlet-->
    <jakarta-servlet.version>4.0.3</jakarta-servlet.version>
    <jersey.version>2.36</jersey.version>
</properties>
```

> [!IMPORTANT]
Spring boot parent pom provides `jakarta-servlet.version` and `jersey.version` versions.
These must be overridden in your pom as mentioned above otherwise you will get exception java.lang.ClassNotFoundException: javax.servlet.http.HttpServlet.
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-spark</artifactId>
    <version>${spring-boot-starter-spark.version}</version>
</dependency>
```

**See example usage in a [Spark Spring could task](https://github.com/officiallysingh/spark-try-me)**

## Spark Configurations
Any spark properties can be configured in `application.yml` as follows:

```yaml
spark:
  master: local[*]
  executor:
    instances: 2
    memory: 2g
    cores: 1
  driver:
    memory: 1g
    cores: 1
```

or in `application.properties` as follows:
```properties
spark.master=local[*]
spark.executor.instances=2
spark.executor.memory=2g
spark.executor.cores=1
spark.driver.memory=1g
spark.driver.cores=1
```

## Auto-configuration
The following Spring beans are auto-configured but they are conditional and can be customized as elaborated in the next section.  
For details refer to [**`SparkAutoConfiguration`**](src/main/java/com/ksoot/spark/springframework/boot/autoconfigure/SparkAutoConfiguration.java)
* [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) bean is auto-configured, but if you want to override you can define your own [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) class bean in your application.
* [**`SparkConf`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html) bean is auto-configured with Spark configuration properties using the standard Spring boot mechanism i.e. you can use a variety of external configuration sources including Java properties files, YAML files, environment variables, and command-line arguments.
But if you want to override it, you can define your own [**`SparkConf`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html) class bean in your application.

## Customizations
### Using [**`SparkSessionBuilderCustomizer`**](src/main/java/com/ksoot/spark/springframework/boot/autoconfigure/SparkSessionBuilderCustomizer.java) 
You can customize [**`SparkSession.Builder`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.Builder.html) by adding a beans of type [**`SparkSessionBuilderCustomizer`**](src/main/java/com/ksoot/spark/springframework/boot/autoconfigure/SparkSessionBuilderCustomizer.java) to your application.

```java
@Bean
public SparkSessionBuilderCustomizer enableHiveSupportCustomizer() {
    return SparkSession.Builder::enableHiveSupport;
}
```
###  Using [**`SparkSessionCustomizer`**](src/main/java/com/ksoot/spark/springframework/boot/autoconfigure/SparkSessionCustomizer.java) 
you can customize [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) by adding beans of type [**`SparkSessionCustomizer`**](src/main/java/com/ksoot/spark/springframework/boot/autoconfigure/SparkSessionCustomizer.java) to your application.
Following is an example to register User Defined Function in [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html):

#### Defining UDF
```java
import static org.apache.spark.sql.functions.callUDF;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;

public class UserDefinedFunctions {

    public static final String EXPLODE_DATE_SEQ = "explodeDateSeq";

    static UDF2<LocalDate, LocalDate, List<LocalDate>> explodeDateSeq =
            (start, end) -> {
                long numOfDaysBetween = ChronoUnit.DAYS.between(start, end) + 1;
                return Stream.iterate(start, date -> date.plusDays(1)).limit(numOfDaysBetween).toList();
            };

    public static Column explodeDateSeq(final Column startCol, final Column endCol) {
        return functions.explode(callUDF(UserDefinedFunctions.EXPLODE_DATE_SEQ, startCol, endCol));
    }
}
```

#### Registering UDF
```java
import com.ksoot.spark.springframework.boot.autoconfigure.SparkSessionCustomizer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

@Component
class SparkSessionUDFCustomizer implements SparkSessionCustomizer, Ordered {

    @Override
    public void customize(final SparkSession sparkSession) {
        sparkSession
                .udf()
                .register(
                        UserDefinedFunctions.EXPLODE_DATE_SEQ,
                        UserDefinedFunctions.explodeDateSeq,
                        DataTypes.createArrayType(DataTypes.DateType));
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
```

#### Calling UDF.
```java
Dataset<Row> originalDf =
    this.sparkSession.createDataFrame(
            Arrays.asList(
                    Dataframe.of("c1", LocalDate.of(2024, 6, 5), "f105"),
                    Dataframe.of("c1", LocalDate.of(2024, 6, 6), "f106"),
                    Dataframe.of("c1", LocalDate.of(2024, 6, 7), "f107"),
                    Dataframe.of("c1", LocalDate.of(2024, 6, 10), "f110"),
                    Dataframe.of("c2", LocalDate.of(2024, 6, 12), "f212"),
                    Dataframe.of("c2", LocalDate.of(2024, 6, 13), "f213"),
                    Dataframe.of("c2", LocalDate.of(2024, 6, 15), "f215")),
            Dataframe.class);

Dataset<Row> customerMinMaxDateDf =
        originalDf
                .groupBy("customer_id")
                .agg(min("date").as("min_date"), max("date").as("max_date"));

// Generate the expanded dataset
Dataset<Row> customerIdDatesDf =
        customerMinMaxDateDf
                .withColumn(
                        "date",
                        UserDefinedFunctions.explodeDateSeq(
                                customerMinMaxDateDf.col("min_date"), customerMinMaxDateDf.col("max_date")))
                .select("customer_id", "date");

customerIdDatesDf.show();   
```
> [!NOTE]
> To support java 8 datetime in Spark, set property `spark.sql.datetime.java8API.enabled` as `true` in `application.yml` or `application.properties`

## Override default beans
#### Override default `sparkProperties` bean as follows with your custom implementation.  
Make sure either the bean definition method name or explicitly specified bean name is also `sparkProperties`
```java
@Bean
Properties sparkProperties() {
    Properties sparkProperties = new Properties();
    sparkProperties.put("spark.master", "local[*]");
    return sparkProperties;
}
```

#### Override default [**`SparkConf`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkConf.html) bean as follows with your custom implementation.
```java
@Bean
SparkConf sparkConf() {
    final SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.master", "local[*]");
    return sparkConf;
}
```

#### Override default [**`SparkSession.Builder`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.Builder.html) bean as follows with your custom implementation.
```java
@Bean
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
SparkSession.Builder sparkSessionBuilder() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.master", "local[*]");
    return builder = SparkSession.builder().config(sparkConf);
}
```

#### Override default [**`SparkSession`**](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/SparkSession.html) bean as follows with your custom implementation.
```java
@Bean(destroyMethod = "stop")
SparkSession sparkSession() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.master", "local[*]");
    final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    return sparkSession;
}
```

## Licence
Open source [**The MIT License**](http://www.opensource.org/licenses/mit-license.php)

## Authors and acknowledgment
[**Rajveer Singh**](https://www.linkedin.com/in/rajveer-singh-589b3950/), In case you find any issues or need any support, please email me at raj14.1984@gmail.com.
Please give me a :star: if you find it helpful.

## References
* To know about spark Refer to [**Spark Documentation**](https://spark.apache.org/docs/3.5.3/).
* Find all spark configurations details at [**Spark Configuration Documentation**](https://spark.apache.org/docs/3.5.3/configuration.html)