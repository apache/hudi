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

package org.apache.hudi.table;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.AppendHandleFactory;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.io.WriteHandleFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract of bucket index bulk_insert partitioner
 */
public abstract class BucketIndexBulkInsertPartitioner<T> extends BucketSortBulkInsertPartitioner<T> {

  public static final Logger LOG = LogManager.getLogger(BucketIndexBulkInsertPartitioner.class);

  private final boolean preserveHoodieMetadata;

  protected final boolean consistentLogicalTimestampEnabled;
  protected final List<String> indexKeyFields;
  protected final List<Boolean> doAppend = new ArrayList<>();
  protected final List<String> fileIdPfxList = new ArrayList<>();
  protected boolean isAppendAllowed;

  public BucketIndexBulkInsertPartitioner(HoodieTable table, String sortString, boolean preserveHoodieMetadata) {
    super(table, sortString);
    this.indexKeyFields = Arrays.asList(table.getConfig().getBucketIndexHashField().split(","));
    this.consistentLogicalTimestampEnabled = table.getConfig().isConsistentLogicalTimestampEnabled();
    this.preserveHoodieMetadata = preserveHoodieMetadata;
    // Multiple bulk inserts into COW using `BucketIndexBulkInsertPartitioner` is restricted, otherwise AppendHandleFactory will produce MOR log files
    this.isAppendAllowed = !table.getMetaClient().getTableConfig().getTableType().equals(HoodieTableType.COPY_ON_WRITE);
  }

  @Override
  public Option<WriteHandleFactory> getWriteHandleFactory(int idx) {
    if (!doAppend.get(idx)) {
      return Option.of(new SingleFileHandleCreateFactory(FSUtils.createNewFileId(getFileIdPfx(idx), 0), this.preserveHoodieMetadata));
    } else if (isAppendAllowed) {
      return Option.of(new AppendHandleFactory());
    } else {
      throw new HoodieNotSupportedException("Multiple bulk inserts into COW with simple bucket and disabled Spark native row writer is not supported, "
          + "please, use upsert operation, overwrite mode (already written data will be lost), or turn on Spark native row writer.");
    }
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    return fileIdPfxList.get(partitionId);
  }

}
