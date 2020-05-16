package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;

public class DummySchemaProvider extends SchemaProvider {

  public DummySchemaProvider(TypedProperties properties) {
    super(properties, null);
  }

  public Schema getSourceSchema() {
    return new Schema.Parser().parse("{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":"
        + "[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":\"int\"},{\"name\":\"favorite_color\",\"type\":\"string\"}]}");
  }
}
