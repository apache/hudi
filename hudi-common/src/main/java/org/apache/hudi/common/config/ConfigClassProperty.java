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

package org.apache.hudi.common.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for superclasses of {@link HoodieConfig} that includes the
 * human-readable name of the config class, the config group ({@link ConfigGroups})
 * it belongs to (e.g., spark/ flink/ write), optional sub-group ({@link ConfigGroups}),
 * and the description of the config class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConfigClassProperty {
  String name();

  ConfigGroups.Names groupName();

  ConfigGroups.SubGroupNames subGroupName() default ConfigGroups.SubGroupNames.NONE;

  boolean areCommonConfigs() default false;

  String description();
}
