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

package org.apache.hudi.integ.testsuite.writer;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * A factory to help instantiate different {@link DeltaWriterAdapter}s depending on the {@link DeltaOutputMode} and {@link DeltaInputType}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaWriterFactory {

  public static DeltaWriterAdapter getDeltaWriterAdapter(DeltaConfig config, Integer batchId) throws IOException {
    switch (config.getDeltaOutputMode()) {
      case DFS:
        switch (config.getDeltaInputType()) {
          case AVRO:
            DFSDeltaConfig dfsDeltaConfig = (DFSDeltaConfig) config;
            dfsDeltaConfig.setBatchId(batchId);
            DeltaInputWriter<GenericRecord> fileDeltaInputGenerator = new AvroFileDeltaInputWriter(
                dfsDeltaConfig.getConfiguration(),
                StringUtils
                    .join(new String[] {dfsDeltaConfig.getDeltaBasePath(), dfsDeltaConfig.getBatchId().toString()},
                        "/"), dfsDeltaConfig.getSchemaStr(), dfsDeltaConfig.getMaxFileSize());
            return new DFSDeltaWriterAdapter(fileDeltaInputGenerator, batchId);
          default:
            throw new IllegalArgumentException("Invalid delta input format " + config.getDeltaInputType());
        }
      default:
        throw new IllegalArgumentException("Invalid delta input type " + config.getDeltaOutputMode());
    }
  }
}
