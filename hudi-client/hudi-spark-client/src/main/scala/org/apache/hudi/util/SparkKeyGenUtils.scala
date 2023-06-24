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
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.{BaseKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator, GlobalAvroDeleteKeyGenerator, GlobalDeleteKeyGenerator, KeyGenerator, NonpartitionedAvroKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName

import scala.collection.JavaConverters._

object SparkKeyGenUtils {

  /**
   * @param properties config properties
   * @return partition columns
   */
  def getPartitionColumns(props: TypedProperties): String = {
    val keyGeneratorClass = getKeyGeneratorClassName(props)
    getPartitionColumns(keyGeneratorClass, props)
  }

  /**
   * @param keyGen key generator class name
   * @return partition columns
   */
  def getPartitionColumns(keyGenClass: String, typedProperties: TypedProperties): String = {
    // For CustomKeyGenerator and CustomAvroKeyGenerator, the partition path filed format
    // is: "field_name: field_type", we extract the field_name from the partition path field.
    if (keyGenClass.equals(classOf[CustomKeyGenerator].getCanonicalName) || keyGenClass.equals(classOf[CustomAvroKeyGenerator].getCanonicalName)) {
      typedProperties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
        .split(",").map(pathField => {
        pathField.split(CustomAvroKeyGenerator.SPLIT_REGEX)
          .headOption.getOrElse(s"Illegal partition path field format: '$pathField' for ${keyGenClass}")}).mkString(",")
    } else if (keyGenClass.equals(classOf[NonpartitionedKeyGenerator].getCanonicalName)
      || keyGenClass.equals(classOf[NonpartitionedAvroKeyGenerator].getCanonicalName)
      || keyGenClass.equals(classOf[GlobalDeleteKeyGenerator].getCanonicalName)
      || keyGenClass.equals(classOf[GlobalAvroDeleteKeyGenerator].getCanonicalName)) {
      StringUtils.EMPTY_STRING
    } else {
      checkArgument(typedProperties.containsKey(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()), "Partition path needs to be set")
      typedProperties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
    }
  }
}
