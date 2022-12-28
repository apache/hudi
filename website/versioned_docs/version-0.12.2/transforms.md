---
title: Transformers
toc: true
---

Apache Hudi provides a HoodieTransformer Utility that allows you to perform transformations the source data before writing it to a Hudi table.
There are several [out-of-the-box](https://github.com/apache/hudi/tree/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/transform)
transformers available and you can build your own custom transformer class as well.

### SQL Query Transformer
You can pass a SQL Query to be executed during write.

```scala
--transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer
--hoodie-conf hoodie.deltastreamer.transformer.sql=SELECT a.col1, a.col3, a.col4 FROM <SRC> a
```

### SQL File Transformer
You can specify a File with a SQL script to be executed during write. The SQL file is configured with this hoodie property:
hoodie.deltastreamer.transformer.sql.file

The query should reference the source as a table named "\<SRC\>"
 
The final sql statement result is used as the write payload.
 
Example Spark SQL Query:
```sql
CACHE TABLE tmp_personal_trips AS
SELECT * FROM <SRC> WHERE trip_type='personal_trips';

SELECT * FROM tmp_personal_trips;
```

### Flattening Transformer
This transformer can flatten nested objects. It flattens the nested fields in the incoming records by prefixing 
inner-fields with outer-field and _ in a nested fashion. Currently flattening of arrays is not supported.

An example schema may look something like the below where name is a nested field of StructType in the original source
```scala
age as intColumn,address as stringColumn,name.first as name_first,name.last as name_last, name.middle as name_middle
```

Set the config as:
```scala
--transformer-class org.apache.hudi.utilities.transform.FlatteningTransformer
```

### Chained Transformer
If you wish to use multiple transformers together, you can use the Chained transformers to pass multiple to be executed sequentially.

Example below first flattens the incoming records and then does sql projection based on the query specified:
```scala
--transformer-class org.apache.hudi.utilities.transform.FlatteningTransformer,org.apache.hudi.utilities.transform.SqlQueryBasedTransformer   
--hoodie-conf hoodie.deltastreamer.transformer.sql=SELECT a.col1, a.col3, a.col4 FROM <SRC> a
```

### AWS DMS Transformer
This transformer is specific for AWS DMS data. It adds `Op` field with value `I` if the field is not present.

Set the config as:
```scala
--transformer-class org.apache.hudi.utilities.transform.AWSDmsTransformer
```

### Custom Transformer Implementation
You can write your own custom transformer by extending [this class](https://github.com/apache/hudi/tree/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/transform)
