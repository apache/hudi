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

import org.apache.hudi.common.util.PartitionPathEncodeUtils;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.function.Supplier;

import static org.apache.hudi.keygen.BuiltinKeyGenerator.toUTF8String;
import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;

/**
 * @inheritDoc
 */
public class UTF8StringPartitionPathFormatter extends PartitionPathFormatterBase<UTF8String> {

  protected static final UTF8String HUDI_DEFAULT_PARTITION_PATH_UTF8 = UTF8String.fromString(HUDI_DEFAULT_PARTITION_PATH);

  public UTF8StringPartitionPathFormatter(Supplier<StringBuilder<UTF8String>> stringBuilderFactory,
                                   boolean useHiveStylePartitioning,
                                   boolean useEncoding) {
    super(stringBuilderFactory, useHiveStylePartitioning, useEncoding);
  }

  @Override
  protected UTF8String toString(Object o) {
    return toUTF8String(o);
  }

  @Override
  protected UTF8String encode(UTF8String partitionPathPart) {
    return UTF8String.fromString(PartitionPathEncodeUtils.escapePathName(partitionPathPart.toString()));
  }

  @Override
  protected UTF8String handleEmpty(UTF8String partitionPathPart) {
    if (partitionPathPart == null || partitionPathPart.numChars() == 0) {
      return HUDI_DEFAULT_PARTITION_PATH_UTF8;
    }

    return partitionPathPart;
  }

  public static class UTF8StringBuilder implements StringBuilder<UTF8String> {
    private final org.apache.hudi.unsafe.UTF8StringBuilder sb = new org.apache.hudi.unsafe.UTF8StringBuilder();

    @Override
    public PartitionPathFormatterBase.StringBuilder<UTF8String> appendJava(String s) {
      sb.append(s);
      return this;
    }

    @Override
    public PartitionPathFormatterBase.StringBuilder<UTF8String> append(UTF8String s) {
      sb.append(s);
      return this;
    }

    @Override
    public UTF8String build() {
      return sb.build();
    }
  }
}
