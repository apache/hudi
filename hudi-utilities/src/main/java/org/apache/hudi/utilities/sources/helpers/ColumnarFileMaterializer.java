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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.hudi.utilities.config.CloudSourceConfig.CLOUD_DATAFILE_EXTENSION;

/**
 * Reads cloud objects as data files through a Spark datasource, which is what every cloud
 * incremental source did before materializers existed. Selects by the configured data file
 * extension, defaulting to the data file format, and sizes partitions by the bytes referenced.
 * This is the default materializer, so leaving it unset preserves existing behaviour.
 */
public class ColumnarFileMaterializer implements CloudObjectMaterializer {

  private static final long serialVersionUID = 1L;

  private final transient CloudObjectsSelectorCommon selectorCommon;
  private final String fileFormat;

  public ColumnarFileMaterializer(TypedProperties props) {
    this(props, new CloudObjectsSelectorCommon(props));
  }

  public ColumnarFileMaterializer(TypedProperties props, CloudObjectsSelectorCommon selectorCommon) {
    this.selectorCommon = selectorCommon;
    this.fileFormat = CloudDataFetcher.getFileFormat(props);
  }

  @Override
  public String objectKeyPredicate(String objectKey, TypedProperties props) {
    return CloudObjectsSelectorCommon.extensionPredicate(
        objectKey, CloudObjectsSelectorCommon.configuredValue(props, CLOUD_DATAFILE_EXTENSION)
            .orElse(fileFormat));
  }

  @Override
  public int partitionCount(List<CloudObjectMetadata> objects, long bytesPerPartition, int minPartitions) {
    long totalSize = 0;
    for (CloudObjectMetadata o : objects) {
      totalSize += o.getSize();
    }
    // inflate 10% for potential hoodie meta fields
    double totalSizeWithHoodieMetaFields = totalSize * 1.1;
    int numPartitions = (int) Math.max(Math.ceil(totalSizeWithHoodieMetaFields / bytesPerPartition), 1);
    return Math.max(numPartitions, minPartitions);
  }

  @Override
  public Option<Dataset<Row>> materialize(SparkSession spark,
                                          List<CloudObjectMetadata> objects,
                                          Option<SchemaProvider> schemaProvider,
                                          int numPartitions) {
    return selectorCommon.loadAsDataset(spark, objects, fileFormat, schemaProvider, numPartitions);
  }
}
