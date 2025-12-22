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

import org.apache.hudi.common.util.SerializationUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * This class is introduced to override the Flink {@link KryoSerializer} within same package path for
 * testing scope only.
 *
 * <p>Flink 2.0 uses Kryo 5.6.2 for {@link KryoSerializer}, but tests in hudi repository uses lower
 * version 4.0.2. To make it compatible regarding Kryo APIs, a simplified {@link KryoSerializer} is
 * implemented here to overwrite the one in Flink, and it can be removed when version of Kryo in hudi
 * is upgraded to 5.6.2 or higher.
 */
public class KryoSerializer<T> extends TypeSerializer<T> {

  private final Class<T> type;
  private static final ThreadLocal<Kryo> KRYO_REF =
      ThreadLocal.withInitial(() -> {
        SerializationUtils.KryoInstantiator kryoInstantiator = new SerializationUtils.KryoInstantiator();
        return kryoInstantiator.newKryo();
      });

  public KryoSerializer(Class<T> type, SerializerConfig serializerConfig) {
    this.type = type;
  }

  private Kryo getKryo() {
    return KRYO_REF.get();
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return new KryoSerializer<>(type, null);
  }

  @Override
  public T createInstance() {
    return getKryo().newInstance(type);
  }

  @Override
  public T copy(T from) {
    if (from == null) {
      return null;
    }
    return getKryo().copy(from);
  }

  @Override
  public T copy(T from, T reuse) {
    return copy(from);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(T record, DataOutputView target) {
    DataOutputViewStream outputStream = new DataOutputViewStream(target);
    try (Output output = new Output(outputStream)) {
      getKryo().writeClassAndObject(output, record);
    }
  }

  @Override
  public T deserialize(DataInputView source) {
    DataInputViewStream inputStream = new DataInputViewStream(source);
    try (Input input = new NoFetchingInput(inputStream)) {
      return (T) getKryo().readClassAndObject(input);
    }
  }

  @Override
  public T deserialize(T reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    T tmp = deserialize(source);
    serialize(tmp, target);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KryoSerializer) {
      KryoSerializer<?> other = (KryoSerializer<?>) obj;

      return type == other.type;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    return new KryoSerializerSnapshot<>(type);
  }
}
