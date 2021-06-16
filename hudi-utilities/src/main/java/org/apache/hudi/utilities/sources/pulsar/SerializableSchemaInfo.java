/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.pulsar;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class SerializableSchemaInfo implements Externalizable {

  private transient SchemaInfo schemaInfo;

  public SerializableSchemaInfo() {
  }

  public SerializableSchemaInfo(SchemaInfo info) {
    this.schemaInfo = info;
  }

  public SchemaInfo get() {
    return schemaInfo;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    byte[] schema = schemaInfo.getSchema();
    out.writeInt(schema.length);
    if (schema.length > 0) {
      out.write(schema);
    }
    out.writeInt(schemaInfo.getType().getValue());
    Map<String, String> props = schemaInfo.getProperties();

    out.writeInt(props.size());
    for (Map.Entry<String, String> e : props.entrySet()) {
      out.writeUTF(e.getKey());
      out.writeUTF(e.getValue());
    }
    out.writeUTF(schemaInfo.getName());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    this.schemaInfo = new SchemaInfo();
    int schemaLen = in.readInt();
    byte[] schema = new byte[schemaLen];
    if (schemaLen > 0) {
      in.readFully(schema);
    }
    schemaInfo.setSchema(schema);
    schemaInfo.setType(SchemaType.valueOf(in.readInt()));

    int numProps = in.readInt();
    Map<String, String> props = new HashMap<>(numProps);
    for (int i = 0; i < numProps; i++) {
      props.put(in.readUTF(), in.readUTF());
    }
    schemaInfo.setProperties(props);
    schemaInfo.setName(in.readUTF());
  }
}
