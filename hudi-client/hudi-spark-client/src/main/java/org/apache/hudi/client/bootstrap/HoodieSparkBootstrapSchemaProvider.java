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

package org.apache.hudi.client.bootstrap;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Objects;

public class HoodieSparkBootstrapSchemaProvider extends HoodieBootstrapSchemaProvider {
  public HoodieSparkBootstrapSchemaProvider(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  protected Schema getBootstrapSourceSchema(HoodieEngineContext context, List<Pair<String, List<HoodieFileStatus>>> partitions) {
    MessageType parquetSchema = partitions.stream().flatMap(p -> p.getValue().stream()).map(fs -> {
      try {
        Path filePath = FileStatusUtils.toPath(fs.getPath());
        return ParquetUtils.readSchema(context.getHadoopConf().get(), filePath);
      } catch (Exception ex) {
        return null;
      }
    }).filter(Objects::nonNull).findAny()
        .orElseThrow(() -> new HoodieException("Could not determine schema from the data files."));


    ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(
        Boolean.parseBoolean(SQLConf.PARQUET_BINARY_AS_STRING().defaultValueString()),
        Boolean.parseBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().defaultValueString()));
    StructType sparkSchema = converter.convert(parquetSchema);
    String tableName = HoodieAvroUtils.sanitizeName(writeConfig.getTableName());
    String structName = tableName + "_record";
    String recordNamespace = "hoodie." + tableName;

    return SchemaConverters.toAvroType(sparkSchema, false, structName, recordNamespace);
  }
}
