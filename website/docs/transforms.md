---
title: Transformers
toc: true
---

Apache Hudi provides a HoodieTransformer Utility that allows you to perform transformations the source data before writing it to a Hudi table.
There are several [out-of-the-box](https://github.com/apache/hudi/tree/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/transform)
transformers available and you can build your own custom transformer class as well.

### SQL Query Transformer
You can pass a SQL Query to be executed during write.

*[TODO: Add Example]*

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
This transformer can flatten nested objects

*[TODO: Add Example]*

### Chained Transformer
If you wish to use multiple transformers together, you can use the Chained transformers to execute them sequentially

*[TODO: Add Example]*

### AWS DMS Transformer
This transformer is specific for AWS DMS data. It adds `Op` field with value `I` if the field is not present.

*[TODO: Add Example]*

### Custom Transformer Implementation
You can write your own custom transformer by extending [this class](https://github.com/apache/hudi/tree/master/hudi-utilities/src/main/java/org/apache/hudi/utilities/transform)

*[TODO: Add Example]*