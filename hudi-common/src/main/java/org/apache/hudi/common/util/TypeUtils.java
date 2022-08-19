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

package org.apache.hudi.common.util;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TypeUtils {

  private TypeUtils() {}

  /**
   * Maps values from the provided Enum's {@link Class} into corresponding values,
   * extracted by provided {@code valueMapper}
   */
  public static <EnumT extends Enum<EnumT>> Map<String, EnumT> getValueToEnumMap(
      @Nonnull Class<EnumT> klass,
      @Nonnull Function<EnumT, String> valueMapper
  ) {
    return Arrays.stream(klass.getEnumConstants())
        .collect(Collectors.toMap(valueMapper, Function.identity()));
  }

  /**
   * This utility abstracts unsafe type-casting in a way that allows to
   * <ul>
   *   <li>Search for such type-casts more easily (just searching for usages of this method)</li>
   *   <li>Avoid type-cast warnings from the compiler</li>
   * </ul>
   */
  @SuppressWarnings("unchecked")
  public static <T> T unsafeCast(Object o) {
    return (T) o;
  }

}
