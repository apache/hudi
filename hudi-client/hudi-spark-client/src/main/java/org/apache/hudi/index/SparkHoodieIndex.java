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

package org.apache.hudi.index;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndex;
import org.apache.hudi.index.bloom.SparkHoodieGlobalBloomIndex;
import org.apache.hudi.index.hbase.SparkHoodieHBaseIndex;
import org.apache.hudi.index.simple.SparkHoodieGlobalSimpleIndex;
import org.apache.hudi.index.simple.SparkHoodieSimpleIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

@SuppressWarnings("checkstyle:LineLength")
public abstract class SparkHoodieIndex<T extends HoodieRecordPayload> extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  protected SparkHoodieIndex(HoodieWriteConfig config) {
    super(config);
  }

  public static SparkHoodieIndex createIndex(HoodieWriteConfig config) {
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      Object instance = ReflectionUtils.loadClass(config.getIndexClass(), config);
      if (!(instance instanceof HoodieIndex)) {
        throw new HoodieIndexException(config.getIndexClass() + " is not a subclass of HoodieIndex");
      }
      return (SparkHoodieIndex) instance;
    }
    switch (config.getIndexType()) {
      case HBASE:
        return new SparkHoodieHBaseIndex<>(config);
      case INMEMORY:
        return new SparkInMemoryHashIndex(config);
      case BLOOM:
        return new SparkHoodieBloomIndex<>(config);
      case GLOBAL_BLOOM:
        return new SparkHoodieGlobalBloomIndex<>(config);
      case SIMPLE:
        return new SparkHoodieSimpleIndex(config);
      case GLOBAL_SIMPLE:
        return new SparkHoodieGlobalSimpleIndex(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                                      HoodieEngineContext context,
                                                      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) throws HoodieIndexException;

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> records,
                                                       HoodieEngineContext context,
                                                       HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) throws HoodieIndexException;
}
