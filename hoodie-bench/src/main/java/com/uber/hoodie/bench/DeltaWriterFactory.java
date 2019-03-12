/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench;

import com.uber.hoodie.bench.configuration.DFSDeltaConfig;
import com.uber.hoodie.bench.configuration.DeltaConfig;
import com.uber.hoodie.bench.writer.AvroDeltaInputWriter;
import com.uber.hoodie.bench.writer.FileDeltaInputWriter;
import com.uber.hoodie.common.util.StringUtils;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;

/**
 * A factory to help instantiate different {@link DeltaWriterAdapter}s depending on the {@link DeltaSinkType} and
 * {@link DeltaInputFormat}
 */
public class DeltaWriterFactory {

  private DeltaWriterFactory() {
  }

  public static DeltaWriterAdapter getDeltaWriterAdapter(DeltaConfig config, Integer batchId) throws IOException {
    switch (config.getDeltaSinkType()) {
      case DFS:
        switch (config.getDeltaInputFormat()) {
          case AVRO:
            DFSDeltaConfig dfsDeltaConfig = (DFSDeltaConfig) config;
            dfsDeltaConfig.setBatchId(batchId);
            FileDeltaInputWriter<GenericRecord> fileDeltaInputGenerator = new AvroDeltaInputWriter(
                dfsDeltaConfig.getConfiguration(),
                StringUtils
                    .join(new String[]{dfsDeltaConfig.getDeltaBasePath(), dfsDeltaConfig.getBatchId().toString()},
                        "/"), dfsDeltaConfig.getSchemaStr(), dfsDeltaConfig.getMaxFileSize());
            DFSDeltaWriterAdapter workloadSink = new DFSDeltaWriterAdapter(fileDeltaInputGenerator);
            return workloadSink;
          default:
            throw new IllegalArgumentException("Invalid delta input format " + config.getDeltaInputFormat());
        }
      default:
        throw new IllegalArgumentException("Invalid delta input type " + config.getDeltaSinkType());
    }
  }
}
