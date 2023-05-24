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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH;

/**
 * Upgrade handler to upgrade Hudi's table version from 4 to 5.
 */
public class FourToFiveUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FourToFiveUpgradeHandler.class);

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    try {
      FileSystem fs = new Path(config.getBasePath()).getFileSystem(context.getHadoopConf().get());
      HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
      String partitionPath = generatorPartitionPath(config, table.getMetaClient());

      if (!config.doSkipDefaultPartitionValidation() && fs.exists(new Path(config.getBasePath() + "/" + partitionPath))) {
        LOG.error(String.format("\"%s\" partition detected. From 0.12, we are changing the default partition in hudi to %s "
                + " Please read and write back the data in \"%s\" partition in hudi to new partition path \"%s\". \"\n"
                + " Sample spark command to use to re-write the data: \n\n"
                + " val df = spark.read.format(\"hudi\").load(HUDI_TABLE_PATH).filter(col(\"PARTITION_PATH_COLUMN\") === \"%s\"); \t \n\n"
                + " df.drop(\"_hoodie_commit_time\").drop(\"_hoodie_commit_seqno\").drop(\"_hoodie_record_key\")\"\n"
                + " .drop(\"_hoodie_partition_path\").drop(\"_hoodie_file_name\").withColumn(PARTITION_PATH_COLUMN,\"%s\")\"\n"
                + " .write.options(writeOptions).mode(Append).save(HUDI_TABLE_PATH);\t\n\"\n"
                + " Please fix values for PARTITION_PATH_COLUMN, HUDI_TABLE_PATH and set all write configs in above command before running. "
                + " Also do delete the records in old partition once above command succeeds. "
                + " Sample spark command to delete old partition records: \n\n"
                + " val df = spark.read.format(\"hudi\").load(HUDI_TABLE_PATH).filter(col(\"PARTITION_PATH_COLUMN\") === \"%s\"); \t \n\n"
                + " df.write.option(\"hoodie.datasource.write.operation\",\"delete\").options(writeOptions).mode(Append).save(HUDI_TABLE_PATH);\t\n\"\n",
            DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH, DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH,
            DEPRECATED_DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH, DEPRECATED_DEFAULT_PARTITION_PATH));
        throw new HoodieException(String.format("Old deprecated \"%s\" partition found in hudi table. This needs a migration step before we can upgrade ",
            DEPRECATED_DEFAULT_PARTITION_PATH));
      }
    } catch (IOException e) {
      LOG.error("Fetching file system instance failed", e);
      throw new HoodieException("Fetching FileSystem instance failed ", e);
    }
    return new HashMap<>();
  }

  private String generatorPartitionPath(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    String partitionPath = DEPRECATED_DEFAULT_PARTITION_PATH;
    try {
      if (tableConfig.getPartitionFields().isPresent()) {
        BaseKeyGenerator keyGenerator =
            (BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps()));

        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        GenericRecord partitionsRecord = new GenericData.Record(schemaResolver.getTableAvroSchema());
        String[] partitions = tableConfig.getPartitionFields().get();
        for (String partition : partitions) {
          partitionsRecord.put(partition, DEPRECATED_DEFAULT_PARTITION_PATH);
        }
        partitionPath = keyGenerator.getPartitionPath(partitionsRecord);
      }
    } catch (Exception e) {
      LOG.error("Load table Schema failed, use no hive style default path", e);
    }
    return partitionPath;
  }
}
