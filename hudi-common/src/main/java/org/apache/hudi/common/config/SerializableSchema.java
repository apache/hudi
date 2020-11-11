/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.config;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapped schema which can be serialized.
 */
public class SerializableSchema implements Serializable {
  private static final long serialVersionUID = -3281148111709753816L;
  private transient Schema schema;

  public SerializableSchema(Schema schema) {
    this.schema = schema;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    WritableUtils.writeVInt(out, 0);
    if (schema != null) {
      List<Schema.Field> fields = schema.getFields();
      WritableUtils.writeVInt(out, fields.size());
      for (Schema.Field field : fields) {
        org.apache.hadoop.io.Text.writeString(out, field.name());
        org.apache.hadoop.io.Text.writeString(out, castSchemaType(field.schema()));
        org.apache.hadoop.io.Text.writeString(out, field.doc() == null ? "" : field.doc());
        out.writeObject(field.defaultVal());
      }
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    int size = WritableUtils.readVInt(in);
    if (size != 0) {
      ArrayList<Schema.Field> fields = new ArrayList<>();
      for (int i = 0; i < size; ++i) {
        String name = org.apache.hadoop.io.Text.readString(in);
        Schema value = castSchemaType(org.apache.hadoop.io.Text.readString(in));
        String doc = org.apache.hadoop.io.Text.readString(in);
        Object defaultValue = in.readObject();
        fields.add(new Schema.Field(name, value, doc, defaultValue));
      }
      this.schema = Schema.createRecord(fields);
    } else {
      schema = null;
    }
  }

  private String castSchemaType(Schema type) {
    return type.getType().getName();
  }

  private Schema castSchemaType(String type) {
    switch (type) {
      case "string":
        return Schema.create(Schema.Type.STRING);
      case "bytes":
        return Schema.create(Schema.Type.BYTES);
      case "int":
        return Schema.create(Schema.Type.INT);
      case "long":
        return Schema.create(Schema.Type.LONG);
      case "float":
        return Schema.create(Schema.Type.FLOAT);
      case "double":
        return Schema.create(Schema.Type.DOUBLE);
      case "boolean":
        return Schema.create(Schema.Type.BOOLEAN);
      case "null":
        return Schema.create(Schema.Type.NULL);
      default:
        throw new AvroRuntimeException("Can't create a: " + type);
    }
  }

  @Override
  public String toString() {
    return schema.toString();
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

}
