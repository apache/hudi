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

package org.apache.spark

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
import org.apache.hudi.common.config.{HoodieCommonConfig, TypedProperties}
import org.apache.hudi.storage.StorageSchemes

import java.util.ArrayList
import scala.collection.JavaConverters
import scala.jdk.CollectionConverters.asScalaSetConverter

object SparkToJavaUtils {

  def convertToJavaMap(scalaMap: Map[String, String]): java.util.Map[String, String] = {
    JavaConverters.mapAsJavaMap(scalaMap)
  }

  // basePath, metaClient.getBasePath.toUri.getScheme, client.getConfig.getCommonConfig.getProps()
  def getLockOptions(tablePath: String, scheme: String, lockConfig: TypedProperties): Map[String, String] = {
    val customSupportedFSs = lockConfig.getStringList(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key, ",", new ArrayList[String])
    if (scheme == null || customSupportedFSs.contains(scheme) || StorageSchemes.isAtomicCreationSupported(scheme)) {
      val props = FileSystemBasedLockProvider.getLockConfig(tablePath)
      props.stringPropertyNames.asScala
        .map(key => key -> props.getString(key))
        .toMap
    } else {
      Map.empty
    }
  }
}
