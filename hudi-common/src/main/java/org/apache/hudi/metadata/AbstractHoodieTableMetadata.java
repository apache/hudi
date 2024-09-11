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

package org.apache.hudi.metadata;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.expression.ArrayData;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractHoodieTableMetadata implements HoodieTableMetadata {

  protected transient HoodieEngineContext engineContext;
  protected transient HoodieStorage storage;

  protected final StorageConfiguration<?> storageConf;
  protected final StoragePath dataBasePath;

  // TODO get this from HoodieConfig
  protected final boolean caseSensitive = false;

  public AbstractHoodieTableMetadata(HoodieEngineContext engineContext, HoodieStorage storage, String dataBasePath) {
    this.engineContext = engineContext;
    this.storage = storage;
    this.storageConf = storage.getConf();
    this.dataBasePath = new StoragePath(dataBasePath);
  }

  protected static int getPathPartitionLevel(Types.RecordType partitionFields, String path) {
    if (StringUtils.isNullOrEmpty(path) || partitionFields == null) {
      return 0;
    }

    int level = 1;
    for (int i = 1; i < path.length() - 1; i++) {
      if (path.charAt(i) == StoragePath.SEPARATOR_CHAR) {
        level++;
      }
    }
    if (path.startsWith(StoragePath.SEPARATOR)) {
      level--;
    }
    if (path.endsWith(StoragePath.SEPARATOR)) {
      level--;
    }
    return level;
  }

  protected static ArrayData extractPartitionValues(Types.RecordType partitionFields,
                                                    String relativePartitionPath,
                                                    boolean urlEncodePartitioningEnabled) {
    if (partitionFields.fields().size() == 1) {
      // SinglePartPartitionValue, which might contain slashes.
      String partitionValue = relativePartitionPath.split("=")[1];
      return new ArrayData(Collections.singletonList(Type.fromPartitionString(
          urlEncodePartitioningEnabled ? PartitionPathEncodeUtils.unescapePathName(partitionValue) : partitionValue,
          partitionFields.field(0).type())));
    }

    List<Object> partitionValues;
    String[] partitionFragments = relativePartitionPath.split("/");
    partitionValues = IntStream.range(0, partitionFragments.length)
        .mapToObj(idx -> {
          String partitionValue = partitionFragments[idx].split("=")[1];
          return Type.fromPartitionString(
              urlEncodePartitioningEnabled ? PartitionPathEncodeUtils.unescapePathName(partitionValue) : partitionValue,
              partitionFields.field(idx).type());
        }).collect(Collectors.toList());

    return new ArrayData(partitionValues);
  }
}
