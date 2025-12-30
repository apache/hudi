---
title: "Apache Hudi Key Generators"
excerpt: "Different key generators available with Apache Hudi"
authors: [sivabalan]
category: blog
tags:
- blog
- key generators
- apache hudi
---

Every record in Hudi is uniquely identified by a primary key, which is a pair of record key and partition path where
the record belongs to. Using primary keys, Hudi can impose a) partition level uniqueness integrity constraint
b) enable fast updates and deletes on records. One should choose the partitioning scheme wisely as it could be a
determining factor for your ingestion and query latency.
<!--truncate-->
In general, Hudi supports both partitioned and global indexes. For a dataset with partitioned index(which is most
commonly used), each record is uniquely identified by a pair of record key and partition path. But for a dataset with
global index, each record is uniquely identified by just the record key. There won't be any duplicate record keys across
partitions.

## Key Generators

Hudi provides several key generators out of the box that users can use based on their need, while having a pluggable
implementation for users to implement and use their own KeyGenerator. This blog goes over all different types of key 
generators that are readily available to use.

[Here](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/KeyGenerator.java)
is the interface for KeyGenerator in Hudi for your reference.

Before diving into different types of key generators, let’s go over some of the common configs required to be set for 
key generators.

| Config        | Meaning/purpose|        
| ------------- |:-------------:| 
| ```hoodie.datasource.write.recordkey.field```     | Refers to record key field. This is a mandatory field. | 
| ```hoodie.datasource.write.partitionpath.field```     | Refers to partition path field. This is a mandatory field. | 
| ```hoodie.datasource.write.keygenerator.class``` | Refers to Key generator class(including full path). Could refer to any of the available ones or user defined one. This is a mandatory field. | 
| ```hoodie.datasource.write.partitionpath.urlencode```| When set to true, partition path will be url encoded. Default value is false. |
| ```hoodie.datasource.write.hive_style_partitioning```| When set to true, uses hive style partitioning. Partition field name will be prefixed to the value. Format: “\<partition_path_field_name\>=\<partition_path_value\>”. Default value is false.|

NOTE: 
Please use `hoodie.datasource.write.keygenerator.class` instead of `hoodie.datasource.write.keygenerator.type`. The second config was introduced more recently.
and will internally instantiate the correct KeyGenerator class based on the type name. The second one is intended for ease of use and is being actively worked on.
We still recommend using the first config until it is marked as deprecated.

There are few more configs involved if you are looking for TimestampBasedKeyGenerator. Will cover those in the respective section.

Lets go over different key generators available to be used with Hudi.

### [SimpleKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/SimpleKeyGenerator.java)

Record key refers to one field(column in dataframe) by name and partition path refers to one field (single column in dataframe) 
by name. This is one of the most commonly used one. Values are interpreted as is from dataframe and converted to string.

### [ComplexKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/ComplexKeyGenerator.java)
Both record key and partition paths comprise one or more than one field by name(combination of multiple fields). Fields 
are expected to be comma separated in the config value. For example ```"Hoodie.datasource.write.recordkey.field" : “col1,col4”```

### [GlobalDeleteKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/GlobalDeleteKeyGenerator.java)
Global index deletes do not require partition value. So this key generator avoids using partition value for generating HoodieKey.

### [TimestampBasedKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/TimestampBasedKeyGenerator.java)
This key generator relies on timestamps for the partition field. The field values are interpreted as timestamps 
and not just converted to string while generating partition path value for records.  Record key is same as before where it is chosen by 
field name.  Users are expected to set few more configs to use this KeyGenerator.

Configs to be set:

| Config        | Meaning/purpose |       
| ------------- | -------------|
| ```hoodie.deltastreamer.keygen.timebased.timestamp.type```    | One of the timestamp types supported(UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR) |
| ```hoodie.deltastreamer.keygen.timebased.output.dateformat```| Output date format | 
| ```hoodie.deltastreamer.keygen.timebased.timezone```| Timezone of the data format| 
| ```oodie.deltastreamer.keygen.timebased.input.dateformat```| Input date format |

Let's go over some example values for TimestampBasedKeyGenerator.

#### Timestamp is GMT

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "EPOCHMILLISECONDS"|
|```hoodie.deltastreamer.keygen.timebased.output.dateformat``` | "yyyy-MM-dd hh" |
|```hoodie.deltastreamer.keygen.timebased.timezone```| "GMT+8:00" |

Input Field value: “1578283932000L” <br/>
Partition path generated from key generator: “2020-01-06 12”

If input field value is null for some rows. <br/>
Partition path generated from key generator: “1970-01-01 08”

#### Timestamp is DATE_STRING

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```|  "DATE_STRING"  |
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```|  "yyyy-MM-dd hh" | 
|```hoodie.deltastreamer.keygen.timebased.timezone```|  "GMT+8:00" |
|```hoodie.deltastreamer.keygen.timebased.input.dateformat```|  "yyyy-MM-dd hh:mm:ss" |

Input field value: “2020-01-06 12:12:12” <br/>
Partition path generated from key generator: “2020-01-06 12”

If input field value is null for some rows. <br/>
Partition path generated from key generator: “1970-01-01 12:00:00”
<br/>

#### Scalar examples

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "SCALAR"|
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```| "yyyy-MM-dd hh" |
|```hoodie.deltastreamer.keygen.timebased.timezone```| "GMT" |
|```hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit```| "days" |

Input field value: “20000L” <br/>
Partition path generated from key generator: “2024-10-04 12”

If input field value is null. <br/>
Partition path generated from key generator: “1970-01-02 12”

#### ISO8601WithMsZ with Single Input format

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "DATE_STRING"|
|```hoodie.deltastreamer.keygen.timebased.input.dateformat```| "yyyy-MM-dd'T'HH:mm:ss.SSSZ" |
|```hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex```| "" |
|```hoodie.deltastreamer.keygen.timebased.input.timezone```| "" |
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```| "yyyyMMddHH" |
|```hoodie.deltastreamer.keygen.timebased.output.timezone```| "GMT" |

Input field value: "2020-04-01T13:01:33.428Z" <br/>
Partition path generated from key generator: "2020040113"

#### ISO8601WithMsZ with Multiple Input formats

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "DATE_STRING"|
|```hoodie.deltastreamer.keygen.timebased.input.dateformat```| "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ" |
|```hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex```| "" |
|```hoodie.deltastreamer.keygen.timebased.input.timezone```| "" |
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```| "yyyyMMddHH" |
|```hoodie.deltastreamer.keygen.timebased.output.timezone```| "UTC" |

Input field value: "2020-04-01T13:01:33.428Z" <br/>
Partition path generated from key generator: "2020040113"

#### ISO8601NoMs with offset using multiple input formats

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "DATE_STRING"|
|```hoodie.deltastreamer.keygen.timebased.input.dateformat```| "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ" |
|```hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex```| "" |
|```hoodie.deltastreamer.keygen.timebased.input.timezone```| "" |
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```| "yyyyMMddHH" |
|```hoodie.deltastreamer.keygen.timebased.output.timezone```| "UTC" |

Input field value: "2020-04-01T13:01:33-**05:00**" <br/>
Partition path generated from key generator: "2020040118"

#### Input as short date string and expect date in date format

| Config field | Value |
| ------------- | -------------|
|```hoodie.deltastreamer.keygen.timebased.timestamp.type```| "DATE_STRING"|
|```hoodie.deltastreamer.keygen.timebased.input.dateformat```| "yyyy-MM-dd'T'HH:mm:ssZ,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyyMMdd" |
|```hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex```| "" |
|```hoodie.deltastreamer.keygen.timebased.input.timezone```| "UTC" |
|```hoodie.deltastreamer.keygen.timebased.output.dateformat```| "MM/dd/yyyy" |
|```hoodie.deltastreamer.keygen.timebased.output.timezone```| "UTC" |

Input field value: "220200401" <br/>
Partition path generated from key generator: "04/01/2020"

### [CustomKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/CustomKeyGenerator.java)
This is a generic implementation of KeyGenerator where users are able to leverage the benefits of SimpleKeyGenerator, 
ComplexKeyGenerator and TimestampBasedKeyGenerator all at the same time. One can configure record key and partition 
paths as a single field or a combination of fields. This keyGenerator is particularly useful if you want to define 
complex partition paths involving regular fields and timestamp based fields. It expects value for prop ```"hoodie.datasource.write.partitionpath.field"``` 
in a specific format. The format should be "field1:PartitionKeyType1,field2:PartitionKeyType2..."

The complete partition path is created as 
```<value for field1 basis PartitionKeyType1>/<value for field2 basis PartitionKeyType2> ```
and so on. Each partition key type could either be SIMPLE or TIMESTAMP.

Example config value: ```“field_3:simple,field_5:timestamp”```

RecordKey config value is either single field incase of SimpleKeyGenerator or a comma separate field names if referring to ComplexKeyGenerator.
Eg: “col1” or “col3,col4”.

### [NonpartitionedKeyGenerator](https://github.com/apache/hudi/blob/master/hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/keygen/NonpartitionedKeyGenerator.java)
If your hudi dataset is not partitioned, you could use this “NonpartitionedKeyGenerator” which will return an empty 
partition for all records. In other words, all records go to the same partition (which is empty “”) 

Hope this blog gave you a good understanding of different types of Key Generators available in Apache Hudi. Thanks for your continued support for Hudi's community. 

