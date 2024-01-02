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

import java.util.function.Supplier;

import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;

/**
 * @inheritDoc
 */
public class StringPartitionPathFormatter extends PartitionPathFormatterBase<String> {

  public StringPartitionPathFormatter(Supplier<StringBuilder<String>> stringBuilderFactory,
                               boolean useHiveStylePartitioning,
                               boolean useEncoding) {
    super(stringBuilderFactory, useHiveStylePartitioning, useEncoding);
  }

  @Override
  protected String toString(Object o) {
    return o == null ? null : o.toString();
  }

  @Override
  protected String encode(String partitionPathPart) {
    return PartitionPathEncodeUtils.escapePathName(partitionPathPart);
  }

  @Override
  protected String handleEmpty(String partitionPathPart) {
    if (partitionPathPart == null) {
      return HUDI_DEFAULT_PARTITION_PATH;
    } else {
      // NOTE: [[toString]] is a no-op if key-part was already a [[String]]
      String keyPartStr = partitionPathPart;
      return keyPartStr.isEmpty() ? HUDI_DEFAULT_PARTITION_PATH : keyPartStr;
    }
  }

  public static class JavaStringBuilder implements PartitionPathFormatterBase.StringBuilder<String> {
    private final java.lang.StringBuilder sb = new java.lang.StringBuilder();

    @Override
    public PartitionPathFormatterBase.StringBuilder<String> appendJava(String s) {
      sb.append(s);
      return this;
    }

    @Override
    public String build() {
      return sb.toString();
    }
  }
}
