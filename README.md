# DataFrame IO
DataFrame IO allows you to
* read data from various [sources](core/src/main/scala/dev/mauch/spark/dfio/DataFrameSource.scala)
* transform using various [transformers](core/src/main/scala/dev/mauch/spark/dfio/TransformerParser.scala)
* write data to various [sinks](core/src/main/scala/dev/mauch/spark/dfio/DataFrameSink.scala)

by specifying source, transform and sink URIs.

The URI schema is
```
protocol://host/path?queryParam1=value1&queryParam2=value2
```

The [protocol decides which source or sink is actually used](uri-parser/src/main/scala/dev/mauch/spark/dfio/DataFrameUrlParser.scala).
Currently, the following options for sources and sinks are available:
* [Console](#console) (`console://`)
* [Values](#values) (`values://`)
* [Text](#text) (CSV, TSV) (`text://`)
* [Parquet](#parquet) (`parquet://`)
* [Delta](#delta) (`delta://`)
* [Excel](#excel) (`xlsx://`)
* [Hive](#hive) (`hive://`)
* [Kafka](#kafka) (`kafka://`)
* [Solr](#solr) (`solr://`)

For transformations, the following options are available:
* [Identity](#identity) (`identity://`)
* [SQL](#sql) (`sql://`)
* [SQL-file](#sql-file) (`sql-file://`)

## Running
The `etl` module contains a main class called `dev.mauch.spark.dfio.ETL`.
You can call it e.g. through
```shell
mill etl.run --help
```

## Development
This project uses [Mill](https://mill-build.com/) as build tool.
You can build the standalone JAR using
```shell
mill etl.assembly
```

## Source and Sink URI schemas
The URI schema for sources and sinks generally looks like this
```
dfToReadInto+sourceType://some-host/some-path?additional=parameters
sourceType://some-host/some-path?additional=parameters

dfToPersist+sinkType://some-host/some-path?additional=parameters
sinkType://some-host/some-path?additional=parameters
```
The possible options for `sourceType` and `sinkType` are listed below.
The `dfToReadInto` is used to save the result of reading the source. If not specified, it defaults to `"source"`.
The `dfToPersist` is the name of the DataFrame that should be persisted to the sink. If not specified, it defaults to `"sink"`.

Both `dfToReadInto` and `dfToPersist` are optional.

### Console
```
console://anything
```
The [source](core/src/main/scala/dev/mauch/spark/dfio/ConsoleDataFrameSink.scala) returns an empty DataFrame.

The [sink](core/src/main/scala/dev/mauch/spark/dfio/ConsoleDataFrameSink.scala) prints an excerpt of the DataFrame to the console by calling `.show()`.

### Values
```
values:///?header=foo,bar&values=1,2;3,4
```
The [source](core/src/main/scala/dev/mauch/spark/dfio/ValuesSource.scala) returns a DataFrame with column names specified in `header` and values specified in `values`.

The [sink](core/src/main/scala/dev/mauch/spark/dfio/ValuesSource.scala) prints an excerpt of the DataFrame to the console by calling `.show()`.
### Text
```
text:///path/to/some.csv
```
[Reads/writes CSV or TSV files](core/src/main/scala/dev/mauch/spark/dfio/TextFileDataFrameSource.scala), determined by file extension.

### Parquet
```
parquet:///path/to/parquet/directory
```
[Reads/writes Parquet files](core/src/main/scala/dev/mauch/spark/dfio/ParquetDataFrameSource.scala) in a given directory.

### Delta
```
delta:///path/to/parquet/directory
```
[Reads/writes Delta table directories](delta/src/main/scala/dev/mauch/spark/dfio/DeltaUriParser.scala) in a given directory.

### Excel
```
xlsx:///path/to/some.xlsl
```
[Reads/writes given Excel file](excel/src/main/scala/dev/mauch/spark/dfio/ExcelFileDataFrameSource.scala).

### Hive
```
hive:///some_db/some_table?partitionCols=my_col_a,my_col_b&checkpointCol=my_timestamp_col
```
[Reads/writes the given table](hive/src/main/scala/dev/mauch/spark/dfio/HiveDataFrameSource.scala).
When writing, data is partitioned by the given `partitionCol`.
The `checkpointCol` is used to filter data by recency.

### Kafka
```
kafka://my_broker:1234/some_topic?serde=avro
```
[Read/writes the given Kafka topic](kafka/src/main/scala/dev/mauch/spark/dfio/KafkaDataFrameSource.scala).
The `serde` parameter determines how data is (de-)serialized.
Valid options are
* `json`
* `avro`

`avro` can be used with or without schema.
When using a schema registry, the following configuration is required:
```properties
spark.kafka.schema.registry.url=http://my_schema_registry:8081
# For Basic Auth
spark.kafka.schema.registry.basic.auth.credentials.source="USER_INFO"
spark.kafka.schema.registry.basic.auth.user.info=my_user:my_password
```
For details check [KafkaDataFrameSource](kafka/src/main/scala/dev/mauch/spark/dfio/KafkaDataFrameSource.scala).

### Solr
```
solr:///some_collection?batchSize=5000
```
[Reads/writes the given Solr collection](solr/src/main/scala/dev/mauch/spark/dfio/SolrDataFrameSourceSink.scala).
The batch size specifies the number of documents
that get written at once per Spark worker
and can be used for performance tuning.

The following configuration options can be used:
```properties
# Required
spark.solr.zookeeperUrl=my_zookeeper:2181/solr
# Optional, defaults to jaas.conf
spark.solr.loginConfig=my_jaas.conf
# Optional, defaults to dataframe-io
spark.solr.appName=my-copy-data-app
```

## Transformation URI Schemas

The URI schema for transformations generally looks like this
```
sourceName+sinkName+transformationType://some-host/some-path?additional=parameters
```
The possible options for `transformationType` are listed below.
The `sourceName` is used to read from a previously named intermediate DataFrame for transformations that expect the DataFrame to be passed explicitly.
Sources like the two SQL sources explicitly mention the name of the intermediate DataFrame.
By default it is `"source"`.

The `sinkName` is used to register the result under a name. By default it is `"sink"`.

`sourceName` and `sinkName` can be specified or omitted in the following ways:
```
transformationType:// # No sourceName or sinkName, they default to "source" and "sink"
sourceName+transformationType:// # Only sourceName, sink defaults to "sink"
sourceName+sinkName+transformationType:// # Both sourceName and sinkName given
```

### Identity
```
sourceName+sinkName+identity:///
```
This transformation can be used to rename a DataFrame from `sourceName` to `sinkName`.

### SQL
```
sql:///SELECT%20foo%20AS%20bar%20FROM%20sourceName
```
This transformation allows to apply inline SQL to its input. The SQL has to be URL encoded (e.g. a space is encoded as `%20`).
This is only helpful for short queries.

### SQL-File
```
sql-file:///path/to/query.sql
```
This transformation allows to apply SQL from a file to its input.
It assumes that the table(s) referenced in the SQL have been registered in a previous step using the extended schema
