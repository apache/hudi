---
title: Key Generation
summary: "In this page, we describe key generation in Hudi."
toc: true
last_modified_at:
---

Hudi maintains hoodie keys (record key + partition path) for uniquely identifying a particular record. Key generator class will extract these out of incoming record. Both the tools above have configs to specify the
`hoodie.datasource.write.keygenerator.class` property. For DeltaStreamer this would come from the property file specified in `--props` and
DataSource writer takes this config directly using `DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY()`.
The default value for this config is `SimpleKeyGenerator`. Note: A custom key generator class can be written/provided here as well. Primary key columns should be provided via `RECORDKEY_FIELD_OPT_KEY` option.<br/>

Hudi currently supports different combinations of record keys and partition paths as below -

- Simple record key (consisting of only one field) and simple partition path (with optional hive style partitioning)
- Simple record key and custom timestamp based partition path (with optional hive style partitioning)
- Composite record keys (combination of multiple fields) and composite partition paths
- Composite record keys and timestamp based partition paths (composite also supported)
- Non partitioned table

`CustomKeyGenerator.java` (part of hudi-spark module) class provides great support for generating hoodie keys of all the above listed types. All you need to do is supply values for the following properties properly to create your desired keys -

```java
hoodie.datasource.write.recordkey.field
hoodie.datasource.write.partitionpath.field
hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.CustomKeyGenerator
```

For having composite record keys, you need to provide comma separated fields like
```java
hoodie.datasource.write.recordkey.field=field1,field2
```

This will create your record key in the format `field1:value1,field2:value2` and so on, otherwise you can specify only one field in case of simple record keys. `CustomKeyGenerator` class defines an enum `PartitionKeyType` for configuring partition paths. It can take two possible values - SIMPLE and TIMESTAMP.
The value for `hoodie.datasource.write.partitionpath.field` property in case of partitioned tables needs to be provided in the format `field1:PartitionKeyType1,field2:PartitionKeyType2` and so on. For example, if you want to create partition path using 2 fields `country` and `date` where the latter has timestamp based values and needs to be customised in a given format, you can specify the following

```java
hoodie.datasource.write.partitionpath.field=country:SIMPLE,date:TIMESTAMP
``` 
This will create the partition path in the format `<country_name>/<date>` or `country=<country_name>/date=<date>` depending on whether you want hive style partitioning or not.

`TimestampBasedKeyGenerator` class defines the following properties which can be used for doing the customizations for timestamp based partition paths

```java
hoodie.deltastreamer.keygen.timebased.timestamp.type
  This defines the type of the value that your field contains. It can be in string format or epoch format, for example
hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit
  This defines the granularity of your field, whether it contains the values in seconds or milliseconds
hoodie.deltastreamer.keygen.timebased.input.dateformat
  This defines the custom format in which the values are present in your field, for example yyyy/MM/dd
hoodie.deltastreamer.keygen.timebased.output.dateformat
  This defines the custom format in which you want the partition paths to be created, for example dt=yyyyMMdd
hoodie.deltastreamer.keygen.timebased.timezone
  This defines the timezone which the timestamp based values belong to
```

When keygenerator class is `CustomKeyGenerator`, non partitioned table can be handled by simply leaving the property blank like
```java
hoodie.datasource.write.partitionpath.field=
```

For those on hudi versions < 0.6.0, you can use the following key generator classes for fulfilling your use cases -

- Simple record key (consisting of only one field) and simple partition path (with optional hive style partitioning) - `SimpleKeyGenerator.java`
- Simple record key and custom timestamp based partition path (with optional hive style partitioning) - `TimestampBasedKeyGenerator.java`
- Composite record keys (combination of multiple fields) and composite partition paths - `ComplexKeyGenerator.java`
- Composite record keys and timestamp based partition paths (composite also supported) - You might need to move to 0.6.0 and use `CustomKeyGenerator.java` class
- Non partitioned table - `NonpartitionedKeyGenerator.java`. Non-partitioned tables can currently only have a single key column, [HUDI-1053](https://issues.apache.org/jira/browse/HUDI-1053)
