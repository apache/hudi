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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * An implementation of {@link TypeSerializerSnapshot} used to snapshot {@link KryoSerializer}.
 */
public class KryoSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

  private Class<T> type;

  public KryoSerializerSnapshot(Class<T> type) {
    this.type = type;
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public void writeSnapshot(DataOutputView out) throws IOException {
    out.writeUTF(type.getName());
  }

  @Override
  public void readSnapshot(int readVersion, DataInputView in, ClassLoader cl) throws IOException {
    this.type = readTypeClass(in, cl);
  }

  @Override
  public TypeSerializer<T> restoreSerializer() {
    return new KryoSerializer<>(type, null);
  }

  @Override
  public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializerSnapshot oldSerializerSnapshot) {
    return TypeSerializerSchemaCompatibility.compatibleAsIs();
  }

  private static <T> Class<T> readTypeClass(DataInputView in, ClassLoader userCodeClassLoader)
      throws IOException {
    return InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
  }
}
