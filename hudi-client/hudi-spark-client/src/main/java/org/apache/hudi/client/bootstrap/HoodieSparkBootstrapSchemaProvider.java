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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HoodieSparkBootstrapSchemaProvider extends HoodieBootstrapSchemaProvider {
  public HoodieSparkBootstrapSchemaProvider(HoodieWriteConfig writeConfig) {
    super(writeConfig);
  }

  @Override
  protected HoodieSchema getBootstrapSourceSchema(HoodieEngineContext context, List<Pair<String, List<HoodieFileStatus>>> partitions) {
    HoodieSchema schema = partitions.stream().flatMap(p -> p.getValue().stream()).map(fs -> {
          Path filePath = HadoopFSUtils.toPath(fs.getPath());
          String extension = FSUtils.getFileExtension(filePath.getName());
          if (PARQUET.getFileExtension().equals(extension)) {
            return getBootstrapSourceSchemaParquet(writeConfig, context, filePath);
          } else if (ORC.getFileExtension().equals(extension)) {
            return getBootstrapSourceSchemaOrc(writeConfig, context, filePath);
          } else {
            throw new HoodieException("Could not determine schema from the data files, supported file formats: [ORC, PARQUET].");
          }
        }
    ).filter(Objects::nonNull).findAny()
        .orElseThrow(() -> new HoodieException("Could not determine schema from the data files."));
    return schema;
  }

  private static HoodieSchema getBootstrapSourceSchemaParquet(HoodieWriteConfig writeConfig, HoodieEngineContext context, Path filePath) {
    // NOTE: The type inference of partition column in the parquet table is turned off explicitly,
    // to be consistent with the existing bootstrap behavior, where the partition column is String
    // typed in Hudi table.
    // TODO(HUDI-4932): add a config to allow type inference of partition column in bootstrap and
    //  support other types of partition column as well
    ((HoodieSparkEngineContext) context).getSqlContext()
        .setConf(SQLConf.PARTITION_COLUMN_TYPE_INFERENCE().key(), "false");
    StructType parquetSchema = ((HoodieSparkEngineContext) context).getSqlContext().read()
        .option("basePath", writeConfig.getBootstrapSourceBasePath())
        .parquet(filePath.toString())
        .schema();
    String tableName = HoodieAvroUtils.sanitizeName(writeConfig.getTableName());
    String structName = tableName + "_record";
    String recordNamespace = "hoodie." + tableName;

    return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(parquetSchema, structName, recordNamespace);
  }

  private static HoodieSchema getBootstrapSourceSchemaOrc(HoodieWriteConfig writeConfig, HoodieEngineContext context, Path filePath) {
    Reader orcReader = null;
    try {
      orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(context.getStorageConf().unwrapAs(Configuration.class)));
    } catch (IOException e) {
      throw new HoodieException("Could not determine schema from the ORC data files.");
    }
    TypeDescription orcSchema = orcReader.getSchema();
    String tableName = HoodieAvroUtils.sanitizeName(writeConfig.getTableName());
    String structName = tableName + "_record";
    String recordNamespace = "hoodie." + tableName;
    return HoodieSchema.fromAvroSchema(AvroOrcUtils.createAvroSchemaWithDefaultValue(orcSchema, structName, recordNamespace, true));
  }
  
}
