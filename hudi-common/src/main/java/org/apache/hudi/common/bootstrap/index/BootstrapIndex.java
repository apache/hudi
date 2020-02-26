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

package org.apache.hudi.common.bootstrap.index;

import org.apache.hudi.avro.model.BootstrapIndexInfo;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Bootstrap Index Interface.
 */
public abstract class BootstrapIndex implements Serializable {

  protected final HoodieTableMetaClient metaClient;

  public BootstrapIndex(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public abstract IndexReader createReader();

  public abstract IndexWriter createWriter(String sourceBasePath);

  /**
   * Bootstrap Index Reader Interface.
   */
  public abstract static  class IndexReader implements Serializable, AutoCloseable {

    protected final HoodieTableMetaClient metaClient;

    public IndexReader(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
    }

    /**
     * Return bootstrap index info.
     * @return
     */
    public abstract BootstrapIndexInfo getIndexInfo();

    /**
     * Return Source base path.
     * @return
     */
    public abstract String getSourceBasePath();

    /**
     * Return list of partitions indexed.
     * @return
     */
    public abstract List<String> getIndexedPartitions();

    /**
     * Return list file-ids indexed.
     * @return
     */
    public abstract List<String> getIndexedFileIds();

    /**
     * Lookup bootstrap index by partition.
     * @param partition Partition to lookup
     * @return
     */
    public abstract List<BootstrapSourceFileMapping> getSourceFileMappingForPartition(String partition);

    /**
     * Lookup Bootstrap index by file group ids.
     * @param ids File Group Ids
     * @return
     */
    public abstract Map<HoodieFileGroupId, BootstrapSourceFileMapping> getSourceFileMappingForFileIds(
        List<HoodieFileGroupId> ids);

    public abstract void close();
  }

  /**
   * Bootstrap Index Writer Interface.
   */
  public abstract static class IndexWriter implements AutoCloseable {

    protected final HoodieTableMetaClient metaClient;

    public IndexWriter(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
    }

    /**
     * Writer calls this method before beginning indexing partitions.
     */
    public abstract void begin();

    /**
     * Append bootstrap index entries for next partitions in sorted order.
     * @param hudiPartitionPath    Hudi Partition Path
     * @param bootstrapSourceFileMappings   Bootstrap Source File to Hudi File Id mapping
     */
    public abstract void appendNextPartition(String hudiPartitionPath,
        List<BootstrapSourceFileMapping> bootstrapSourceFileMappings);

    /**
     * Writer calls this method after appending all partitions to be indexed.
     */
    public abstract void finish();

    /**
     * Drop bootstrap index.
     */
    public abstract void dropIndex();

    public abstract void close();
  }
}
