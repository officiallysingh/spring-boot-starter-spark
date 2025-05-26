package com.ksoot.spark.springframework.boot.autoconfigure;

import static com.ksoot.spark.util.SparkConstants.*;

import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.*;
import org.apache.iceberg.catalog.Namespace;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@NoArgsConstructor
@Validated
public class CatalogProperties {

  @NotEmpty private String name;

  @NotNull private Type type = Type.hadoop;

  @NotEmpty private String warehouse = "warehouse";

  @NotEmpty private String uri;

  @NotEmpty private String namespace;

  @NotEmpty private String ioImpl;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private String tablePrefix;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private Namespace nameSpace;

  public static CatalogProperties of(final String name, final Map<String, Object> properties) {
    CatalogProperties catalogProperties = new CatalogProperties();
    catalogProperties.name = name;
    catalogProperties.type =
        properties.containsKey(CATALOG_TYPE)
            ? Type.valueOf(properties.get(CATALOG_TYPE).toString())
            : catalogProperties.type;
    catalogProperties.uri =
        properties.containsKey(org.apache.iceberg.CatalogProperties.URI)
            ? properties.get(org.apache.iceberg.CatalogProperties.URI).toString()
            : catalogProperties.uri;
    catalogProperties.warehouse =
        properties.containsKey(org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION)
            ? properties.get(org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION).toString()
            : catalogProperties.warehouse;
    catalogProperties.namespace =
        properties.containsKey(CATALOG_DEFAULT_NAMESPACE)
            ? properties.get(CATALOG_DEFAULT_NAMESPACE).toString()
            : catalogProperties.namespace;
    catalogProperties.ioImpl =
        properties.containsKey(org.apache.iceberg.CatalogProperties.CATALOG_IMPL)
            ? properties.get(org.apache.iceberg.CatalogProperties.CATALOG_IMPL).toString()
            : catalogProperties.ioImpl;
    return catalogProperties;
  }

  public Namespace namespace() {
    return this.nameSpace;
  }

  public String tablePrefix() {
    return this.tablePrefix;
  }

  public enum Type {
    hadoop,
    hive,
    glue,
    nessie,
    jdbc,
    rest
  }

  @PostConstruct
  void initialize() {
    this.nameSpace = Namespace.of(this.namespace);
    this.tablePrefix = this.name + "." + this.namespace + ".";
  }

  @Override
  public String toString() {
    return "name='"
        + name
        + '\''
        + ", type="
        + type
        + ", warehouse='"
        + warehouse
        + '\''
        + ", uri='"
        + uri
        + '\''
        + ", namespace='"
        + namespace
        + '\''
        + ", ioImpl='"
        + ioImpl
        + '\''
        + ", tablePrefix='"
        + tablePrefix
        + '\''
        + ", nameSpace="
        + nameSpace;
  }
}
