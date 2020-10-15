package org.apache.hudi.utilities.schema;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkAvroPostProcessor extends SchemaPostProcessor {

  protected SparkAvroPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    return AvroConversionUtils.convertStructTypeToAvroSchema(
        AvroConversionUtils.convertAvroSchemaToStructType(schema), RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME, 
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE);
  }
}