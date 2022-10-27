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

package org.apache.hudi.keygen;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR;

/**
 * Partition path formatter allows to efficiently combine partition paths into
 * generic Java {@link String} (as well as engine-specific containers like
 * {@code UTF8String} for ex), while being flexible in terms of
 *
 * <ul>
 *   <li>Allowing to configure how to handle empty values</li>
 *   <li>Allowing to encode individual values</li>
 *   <li>Supporting Hive-style partitioning ({@code column=value})</li>
 * </ul>
 *
 * @param <S> string type
 */
public class PartitionPathFormatter<S> {

  private final Supplier<StringBuilder<S>> stringBuilderFactory;
  private final Function<Object, S> valueConverter;
  private final Function<S, S> encoder;

  private final Function<S, S> emptyHandler;

  private final boolean hiveStylePartitioning;

  PartitionPathFormatter(Supplier<StringBuilder<S>> stringBuilderFactory,
                         Function<Object, S> valueConverter,
                         Function<S, S> encoder,
                         Function<S, S> emptyHandler,
                         boolean hiveStylePartitioning) {
    this.stringBuilderFactory = stringBuilderFactory;
    this.valueConverter = valueConverter;
    this.encoder = encoder;
    this.emptyHandler = emptyHandler;
    this.hiveStylePartitioning = hiveStylePartitioning;
  }

  public final S combine(List<String> partitionPathFields, Object... partitionPathParts) {
    checkState(partitionPathParts.length == partitionPathFields.size());
    // Avoid creating [[StringBuilder]] in case there's just one partition-path part,
    // and Hive-style of partitioning is not required
    if (!hiveStylePartitioning && partitionPathParts.length == 1) {
      return emptyHandler.apply(valueConverter.apply(partitionPathParts[0]));
    }

    StringBuilder<S> sb = stringBuilderFactory.get();
    for (int i = 0; i < partitionPathParts.length; ++i) {
      S partitionPathPartStr = encoder.apply(emptyHandler.apply(valueConverter.apply(partitionPathParts[i])));

      if (hiveStylePartitioning) {
        sb.appendJava(partitionPathFields.get(i))
            .appendJava("=")
            .append(partitionPathPartStr);
      } else {
        sb.append(partitionPathPartStr);
      }

      if (i < partitionPathParts.length - 1) {
        sb.appendJava(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }

    return sb.build();
  }

  /**
   * This is a generic interface closing the gap and unifying the {@link java.lang.StringBuilder} with
   * {@link org.apache.hudi.unsafe.UTF8StringBuilder} implementations, allowing us to avoid code-duplication by performing
   * most of the key-generation in a generic and unified way
   *
   * @param <S> target string type this builder is producing (could either be native {@link String}
   *           or alternatively {@link UTF8String}
   */
  interface StringBuilder<S> {
    default StringBuilder<S> append(S s) {
      return appendJava(s.toString());
    }

    StringBuilder<S> appendJava(String s);

    S build();
  }

  static class JavaStringBuilder implements PartitionPathFormatter.StringBuilder<String> {
    private final java.lang.StringBuilder sb = new java.lang.StringBuilder();

    @Override
    public PartitionPathFormatter.StringBuilder<String> appendJava(String s) {
      sb.append(s);
      return this;
    }

    @Override
    public String build() {
      return sb.toString();
    }
  }

}
