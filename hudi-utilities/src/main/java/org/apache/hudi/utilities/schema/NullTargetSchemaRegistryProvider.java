package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;

public class NullTargetSchemaRegistryProvider extends SchemaRegistryProvider {

  public NullTargetSchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema getTargetSchema() {
    return null;
  }
}
