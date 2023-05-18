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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("???")
public enum HoodieRecordMergerType {

  @EnumFieldDescription("???")
  AVRO("org.apache.hudi.common.model.HoodieAvroRecordMerger"),

  @EnumFieldDescription("???")
  SPARK("org.apache.hudi.HoodieSparkRecordMerger"),

  @EnumFieldDescription("Validate the duplicate key for insert statement without enabling the INSERT_DROP_DUPS config")
  SPARK_VALIDATE_DUPS("org.apache.spark.sql.hudi.command.HoodieSparkValidateDuplicateKeyRecordMerger"),

  @EnumFieldDescription("Uses record merger implementation set in `hoodie.datasource.write.record.merger.impls`")
  CUSTOM("");

  public final String classPath;

  HoodieRecordMergerType(String classPath) {
    this.classPath = classPath;
  }

}
