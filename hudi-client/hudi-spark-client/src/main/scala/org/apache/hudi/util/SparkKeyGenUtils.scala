/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.{AutoRecordKeyGeneratorWrapper, BaseKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator, GlobalAvroDeleteKeyGenerator, GlobalDeleteKeyGenerator, KeyGenerator, NonpartitionedAvroKeyGenerator, NonpartitionedKeyGenerator}

object SparkKeyGenUtils {

  /**
   * @param properties config properties
   * @return partition columns
   */
  def getPartitionColumns(props: TypedProperties): String = {
    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    getPartitionColumns(keyGenerator, props, false)
  }

  /**
   * @param properties config properties
   * @param writerTableVersion table version used by writer
   * @return partition columns
   */
  def getPartitionColumnsForKeyGenerator(props: TypedProperties, writerTableVersion: HoodieTableVersion): String = {
    val keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    getPartitionColumns(keyGenerator, props, writerTableVersion.versionCode() > HoodieTableVersion.SIX.versionCode())
  }

  /**
   * @param KeyGenClassNameOption key generator class name if present.
   * @param props                 config properties.
   * @return partition column names only, concatenated by ","
   */
  def getPartitionColumns(KeyGenClassNameOption: Option[String], props: TypedProperties): String = {
    val keyGenerator = if (KeyGenClassNameOption.isEmpty) {
      HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    } else {
      HoodieSparkKeyGeneratorFactory.createKeyGenerator(KeyGenClassNameOption.get, props)
    }
    getPartitionColumns(keyGenerator, props, false)
  }

  /**
   * @param keyGen key generator class name
   * @return partition columns
   */
  def getPartitionColumns(keyGenClass: KeyGenerator, typedProperties: TypedProperties, includeKeyGenPartitionType: Boolean): String = {
    // For {@link AutoRecordGenWrapperKeyGenerator} or {@link AutoRecordGenWrapperAvroKeyGenerator},
    // get the base key generator for the partition paths
    var baseKeyGen = keyGenClass match {
      case autoRecordKeyGenerator: AutoRecordKeyGeneratorWrapper =>
        autoRecordKeyGenerator.getPartitionKeyGenerator
      case _ => keyGenClass
    }

    // For CustomKeyGenerator and CustomAvroKeyGenerator, the partition path filed format
    // is: "field_name: field_type", we extract the field_name from the partition path field.
    if (baseKeyGen.isInstanceOf[CustomKeyGenerator] || baseKeyGen.isInstanceOf[CustomAvroKeyGenerator]) {
      val partitionFields = typedProperties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
      if (includeKeyGenPartitionType) {
        partitionFields
      } else {
        partitionFields.split(",").map(pathField => {
          pathField.split(BaseKeyGenerator.CUSTOM_KEY_GENERATOR_SPLIT_REGEX)
            .headOption.getOrElse(s"Illegal partition path field format: '$pathField' for ${baseKeyGen}")
        }).mkString(",")
      }
    } else if (baseKeyGen.isInstanceOf[NonpartitionedKeyGenerator]
      || baseKeyGen.isInstanceOf[NonpartitionedAvroKeyGenerator]
      || baseKeyGen.isInstanceOf[GlobalDeleteKeyGenerator]
      || baseKeyGen.isInstanceOf[GlobalAvroDeleteKeyGenerator]) {
      StringUtils.EMPTY_STRING
    } else {
      checkArgument(typedProperties.containsKey(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()), "Partition path needs to be set")
      typedProperties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
    }
  }
}
