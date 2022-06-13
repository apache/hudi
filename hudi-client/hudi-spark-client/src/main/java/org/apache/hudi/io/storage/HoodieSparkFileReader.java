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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.apache.hudi.TypeUtils.unsafeCast;

public interface HoodieSparkFileReader extends HoodieFileReader<InternalRow> {

  Logger LOG = LogManager.getLogger(HoodieSparkFileReader.class);

  ClosableIterator<InternalRow> getInternalRowIterator(Schema readerSchema) throws IOException;

  default ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema) throws IOException {
    ClosableIterator<InternalRow> iterator = getInternalRowIterator(readerSchema);
    try {
      Constructor<?> constructor = ReflectionUtils.getClass("org.apache.hudi.HoodieSparkRecord").getConstructor(InternalRow.class);
      return new MappingIterator<>(iterator, data -> {
        try {
          return unsafeCast(constructor.newInstance(data));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          LOG.error("Can not init spark record", e);
          throw new HoodieException(e);
        }
      });
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    }
  }

  @Override
  default ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    // TODO used in HoodieParquetDataBlock
    return getRecordIterator(readerSchema);
  }
}
