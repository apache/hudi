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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.apache.hudi.client.utils.SparkPartitionUtils.getPartitionFieldVals;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;

public abstract class SparkJobExecutionStrategy<T, I, K, O> extends ClusteringExecutionStrategy<T, I, K, O> {

  protected final Schema readerSchemaWithMetaFields;

  public SparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    this.readerSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(writeConfig.getSchema()));
  }

  protected HoodieFileReader getBaseOrBootstrapFileReader(StorageConfiguration<?> storageConf, String bootstrapBasePath, Option<String[]> partitionFields, ClusteringOperation clusteringOp) {
    StoragePath dataFilePath = new StoragePath(clusteringOp.getDataFilePath());
    HoodieStorage storage = new HoodieHadoopStorage(dataFilePath, storageConf);
    try {
      HoodieFileReader baseFileReader = getHoodieSparkIOFactory(storage).getReaderFactory(recordType)
          .getFileReader(writeConfig, dataFilePath);
      // handle bootstrap path
      if (StringUtils.nonEmpty(clusteringOp.getBootstrapFilePath()) && StringUtils.nonEmpty(bootstrapBasePath)) {
        String bootstrapFilePath = clusteringOp.getBootstrapFilePath();
        Object[] partitionValues = new Object[0];
        if (partitionFields.isPresent()) {
          int startOfPartitionPath = bootstrapFilePath.indexOf(bootstrapBasePath) + bootstrapBasePath.length() + 1;
          String partitionFilePath = bootstrapFilePath.substring(startOfPartitionPath, bootstrapFilePath.lastIndexOf("/"));
          partitionValues = getPartitionFieldVals(partitionFields, partitionFilePath, bootstrapBasePath, baseFileReader.getSchema(),
              storageConf.unwrapAs(Configuration.class));
        }
        baseFileReader = getHoodieSparkIOFactory(storage).getReaderFactory(recordType).newBootstrapFileReader(
            baseFileReader,
            getHoodieSparkIOFactory(storage).getReaderFactory(recordType).getFileReader(
                writeConfig, new StoragePath(bootstrapFilePath)), partitionFields,
            partitionValues);
      }
      return baseFileReader;
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading base file", e);
    }
  }

  @Override
  protected Option<BaseKeyGenerator> getKeyGenerator() {
    return HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(writeConfig);
  }
}
