package com.uber.hoodie.utilities.schema;

import com.uber.hoodie.AvroConversionUtils;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.StructType;

public class RowBasedSchemaProvider extends SchemaProvider {

  // Used in GenericRecord conversions
  public static final String HOODIE_RECORD_NAMESPACE = "hoodie.source";
  public static final String HOODIE_RECORD_STRUCT_NAME = "hoodie_source";

  private StructType rowStruct;

  public RowBasedSchemaProvider(StructType rowStruct) {
    super(null, null);
    this.rowStruct = rowStruct;
  }

  @Override
  public Schema getSourceSchema() {
    return AvroConversionUtils.convertStructTypeToAvroSchema(rowStruct, HOODIE_RECORD_STRUCT_NAME,
            HOODIE_RECORD_NAMESPACE);
  }
}
