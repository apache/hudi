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

package org.apache.hudi.index;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

@SuppressWarnings("checkstyle:LineLength")
public abstract class SparkHoodieIndex<T extends HoodieRecordPayload<T>>
    extends HoodieIndex<JavaRDD<HoodieRecord<T>>, JavaRDD<WriteStatus>> {
  protected SparkHoodieIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public abstract JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                                      HoodieEngineContext context,
                                                      HoodieTable hoodieTable) throws HoodieIndexException;

  @Override
  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  public abstract JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> records,
                                                       HoodieEngineContext context,
                                                       HoodieTable hoodieTable) throws HoodieIndexException;

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException {
    return HoodieJavaRDD.of(tagLocation(
        HoodieJavaRDD.getJavaRDD(records.map(record -> (HoodieRecord<T>) record)), context, hoodieTable)
        .map(r -> (HoodieRecord<R>) r));
  }

  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) throws HoodieIndexException {
    return HoodieJavaRDD.of(updateLocation(HoodieJavaRDD.getJavaRDD(writeStatuses), context, hoodieTable));
  }
}
