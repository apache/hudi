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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieInstantWriter;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.function.BooleanSupplier;

/**
 * Interface for serializing and deserializing commit metadata.
 */
public interface CommitMetadataSerDe extends Serializable {

  <T> T deserialize(HoodieInstant instant, InputStream instantStream, BooleanSupplier isEmptyInstant, Class<T> clazz) throws IOException;

  <T> Option<HoodieInstantWriter> getInstantWriter(T commitMetadata);

  static <T extends SpecificRecordBase> Option<HoodieInstantWriter> getInstantWriter(Option<T> metadata) {
    if (metadata.isEmpty()) {
      return Option.empty();
    }
    return Option.of(outputStream -> {
      DatumWriter<T> datumWriter = (DatumWriter<T>) new SpecificDatumWriter(metadata.get().getClass());
      try (DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter)) {
        fileWriter.create(metadata.get().getSchema(), outputStream);
        fileWriter.append(metadata.get());
      }
    });
  }
}