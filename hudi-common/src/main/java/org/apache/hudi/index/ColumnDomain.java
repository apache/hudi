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

import org.apache.hudi.common.util.Option;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ColumnDomain<T> {

  private final Option<Map<T, Domain>> domains;

  public Option<Map<T, Domain>> getDomains() {
    return domains;
  }

  public ColumnDomain(Option<Map<T, Domain>> domains) {
    requireNonNull(domains, "domains is null");

    this.domains = domains.flatMap(map -> {
      if (containsNoneDomain(map)) {
        return Option.empty();
      }
      return Option.of(unmodifiableMap(normalizeAndCopy(map)));
    });
  }

  private static <T> Map<T, Domain> normalizeAndCopy(Map<T, Domain> domains) {
    return domains.entrySet().stream()
            .filter(entry -> !entry.getValue().isAll())
            .collect(toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return toMap(keyMapper, valueMapper, (u, v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u)); }, LinkedHashMap::new);
  }

  private static <T> boolean containsNoneDomain(Map<T, Domain> domains) {
    return domains.values().stream().anyMatch(Domain::isNone);
  }
}
