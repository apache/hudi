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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/** The source contracts the streamer applies, by source class; registering a source also covers its subclasses. */
public final class SourceContracts {

  /** The contract of a source that registers none: the job's properties are used as given. */
  public static final SourceContract NONE = props -> props;

  private static final Map<Class<?>, Function<TypedProperties, SourceContract>> FACTORIES = Collections.emptyMap();

  private SourceContracts() {
  }

  /** Resolves a registered source without initializing it; normal source loading reports unknown classes. */
  public static SourceContract of(String sourceClassName, TypedProperties props) {
    return of(sourceClassName, props, FACTORIES);
  }

  static SourceContract of(String sourceClassName, TypedProperties props, Map<Class<?>, Function<TypedProperties, SourceContract>> factories) {
    if (StringUtils.isNullOrEmpty(sourceClassName) || factories.isEmpty()) {
      return NONE;
    }
    Class<?> sourceClass;
    try {
      sourceClass = Class.forName(sourceClassName.trim(), false, SourceContracts.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return NONE;
    }
    for (Class<?> type = sourceClass; type != null; type = type.getSuperclass()) {
      Function<TypedProperties, SourceContract> factory = factories.get(type);
      if (factory != null) {
        return factory.apply(props);
      }
    }
    return NONE;
  }
}
