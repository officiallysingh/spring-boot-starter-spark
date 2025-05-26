package com.ksoot.spark.util;

public class SparkOptions {

  public static final class Executor {
    private static final String PREFIX = "spark.executor.";

    public static final String INSTANCES = PREFIX + "instances";
    public static final String MEMORY = PREFIX + "memory";
  }

  public static final class Column {
    public static final String HOW_ALL = "all";
    public static final String HOW_ANY = "any";
  }

  public static final class Common {
    public static final String HEADER = "header"; // inferSchema",
    public static final String INFER_SCHEMA = "inferSchema";
    public static final String PATH = "path";
    public static final String FORMAT = "format";
    public static final String CHECKPOINT_LOCATION = "checkpointLocation";
  }

  public static final class CSV {
    public static final String FORMAT = "csv";
  }

  public static final class Json {
    public static final String FORMAT = "json";
    public static final String MULTILINE = "multiline";
  }

  public static final class Parquet {
    public static final String FORMAT = "parquet";
    public static final String COMPRESSION = "compression";

    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_UNCOMPRESSED = "uncompressed";
    public static final String COMPRESSION_SNAPPY = "snappy";
    public static final String COMPRESSION_GZIP = "gzip";
    public static final String COMPRESSION_LZO = "lzo";
    public static final String COMPRESSION_BROTLI = "brotli";
    public static final String COMPRESSION_LZ4 = "lz4";
    public static final String COMPRESSION_ZSTD = "zstd";
  }

  public static final class Mongo {
    public static final String READ_CONFIG_PREFIX = "spark.mongodb.read.";
    public static final String WRITE_CONFIG_PREFIX = "spark.mongodb.write";
    public static final String FORMAT = "mongodb";
    public static final String DATABASE = "database";
    public static final String COLLECTION = "collection";
    public static final String AGGREGATION_PIPELINE = READ_CONFIG_PREFIX + "aggregation.pipeline";
    public static final String READ_CONNECTION_URI = READ_CONFIG_PREFIX + "connection.uri";
    public static final String WRITE_CONNECTION_URI = WRITE_CONFIG_PREFIX + "connection.uri";
  }

  public static final class Jdbc {
    public static final String FORMAT = "jdbc";
    public static final String URL = "url";
    public static final String DRIVER = "driver";
    public static final String TABLE = "dbtable";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String FETCH_SIZE = "fetchSize";
    public static final String BATCH_SIZE = "batchSize";
    public static final String ISOLATION_LEVEL = "isolationLevel";
  }

  public static final class Join {
    public static final String INNER = "inner";
    public static final String FULL = "full";
    public static final String FULL_OUTER = "full_outer";
    public static final String LEFT = "left";
    public static final String LEFT_OUTER = "left_outer";
    public static final String LEFT_SEMI = "left_semi";
    public static final String LEFT_ANTI = "left_anti";
    public static final String RIGHT = "right";
    public static final String RIGHT_OUTER = "right_outer";
    public static final String CROSS = "cross";
  }

  public static final class Kafka {
    public static final String FORMAT = "kafka";
    public static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String STARTING_OFFSETS = "startingOffsets";
    public static final String SUBSCRIBE = "subscribe";
    public static final String GROUP_ID = "group.id";
    public static final String TOPIC = "topic";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    public static final String FAIL_ON_DATA_LOSS = "failOnDataLoss";
  }

  public static final class Iceberg {
    public static final String PROVIDER = "iceberg";
    public static final String FORMAT = "iceberg";
    public static final String WRITE_FORMAT = "write-format";
    public static final String ISOLATION_LEVEL = "isolation-level";
  }

  public static final class Aws {
    public static final String S3_ACCESS_KEY = "spark.hadoop.fs.s3a.access.key";
    public static final String S3_SECRET_KEY = "spark.hadoop.fs.s3a.secret.key";
    public static final String S3_ENDPOINT = "spark.hadoop.fs.s3a.endpoint";
  }
}
