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

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A wrapped Schema which can be serialized.
 */
public class SerializableSchema implements Serializable {

  private transient Schema schema;
  
  public SerializableSchema() {
  }

  public SerializableSchema(String schemaStr) {
    this.schema = new Schema.Parser().parse(schemaStr);
  }

  public SerializableSchema(Schema schema) {
    this.schema = newCopy(schema);
  }

  public SerializableSchema(SerializableSchema serializableSchema) {
    this(serializableSchema.schema);
  }

  public static Schema newCopy(Schema schemaObject) {
    return new Schema.Parser().parse(schemaObject.toString());
  }

  public Schema get() {
    return schema;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    writeObjectTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    readObjectFrom(in);
  }
  
  // create a public write method for unit test
  public void writeObjectTo(ObjectOutputStream out) throws IOException {
    // Note: writeUTF cannot support string length > 64K. So use writeObject which has small overhead (relatively).
    out.writeObject(schema.toString());
  }

  // create a public read method for unit test
  public void readObjectFrom(ObjectInputStream in) throws IOException {
    try {
      schema = new Schema.Parser().parse(in.readObject().toString());
    } catch (ClassNotFoundException e) {
      throw new IOException("unable to parse schema", e);
    }
  }

  @Override
  public String toString() {
    return schema.toString();
  }
}
