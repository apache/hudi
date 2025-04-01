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

package org.apache.hudi.util

import org.apache.hudi.common.config.ConfigProperty
import org.apache.hudi.common.util.ConfigUtils

object SparkConfigUtils {

  /**
   * Gets the String value for a {@link ConfigProperty} config using ConfigUtils utility class. It checks
   * the alternate config keys for the specified key as well.
   *
   * @param props          Configs in scala map
   * @param configProperty {@link ConfigProperty} config of type T to fetch.
   * @return String value if the config exists; default String value if the config does not exist
   *         and there is default value defined in the {@link ConfigProperty} config; {@code null} otherwise.
   */
  def getStringWithAltKeys[T](props: scala.collection.Map[String, String], configProperty: ConfigProperty[T]): String = {
    ConfigUtils.getStringWithAltKeys(JFunction.toJavaFunction[String, Object](key => props.getOrElse(key, null)), configProperty)
  }

  /**
   * Whether the properties contain a config. If any of the key or alternative keys of the
   * {@link ConfigProperty} exists in the properties, this method returns {@code true}.
   *
   * @param props          Configs in scala map
   * @param configProperty Config to look up.
   * @return {@code true} if exists; {@code false} otherwise.
   */
  def containsConfigProperty(props: Map[String, String], configProperty: ConfigProperty[_]): Boolean = {
    ConfigUtils.containsConfigProperty(JFunction.toJavaFunction[String, java.lang.Boolean](key => props.contains(key)), configProperty)
  }
}
