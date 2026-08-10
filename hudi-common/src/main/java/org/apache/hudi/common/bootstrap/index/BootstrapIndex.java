/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.bootstrap.index;

import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ReflectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Bootstrap Index Interface.
 */
public abstract class BootstrapIndex implements Serializable {

  protected static final long serialVersionUID = 1L;

  protected final HoodieTableMetaClient metaClient;

  public BootstrapIndex(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  /**
   * Create Bootstrap Index Reader.
   * @return Index Reader
   */
  public abstract IndexReader createReader();

  /**
   * Create Bootstrap Index Writer.
   * @param sourceBasePath Source Base Path
   * @return Index Writer
   */
  public abstract IndexWriter createWriter(String sourceBasePath);

  /**
   * Drop bootstrap index.
   */
  public abstract void dropIndex();

  /**
   * Returns true if valid metadata bootstrap is present.
   * @return
   */
  public final boolean useIndex() {
    if (isPresent()) {
      boolean validInstantTime = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant()
          .map(i -> compareTimestamps(i.requestedTime(), GREATER_THAN_OR_EQUALS,
              HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS)).orElse(false);
      return validInstantTime && metaClient.getTableConfig().getBootstrapBasePath().isPresent();
    } else {
      return false;
    }
  }

  /**
   * Check if bootstrap Index is physically present. It does not guarantee the validity of the index.
   * To ensure an index is valid, use useIndex() API.
   */
  protected abstract boolean isPresent();

  /**
   * Bootstrap Index Reader Interface.
   */
  public abstract static class IndexReader implements Serializable, AutoCloseable {

    protected final HoodieTableMetaClient metaClient;

    public IndexReader(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
    }

    /**
     * Return Source base path.
     * @return
     */
    public abstract String getBootstrapBasePath();

    /**
     * Return list of partitions indexed.
     * @return
     */
    public abstract List<String> getIndexedPartitionPaths();

    /**
     * Return list file-ids indexed.
     * @return
     */
    public abstract List<HoodieFileGroupId> getIndexedFileGroupIds();

    /**
     * Lookup bootstrap index by partition.
     * @param partition Partition to lookup
     * @return
     */
    public abstract List<BootstrapFileMapping> getSourceFileMappingForPartition(String partition);

    /**
     * Lookup Bootstrap index by file group ids.
     * @param ids File Group Ids
     * @return
     */
    public abstract Map<HoodieFileGroupId, BootstrapFileMapping> getSourceFileMappingForFileIds(
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
     * @param partitionPath    Partition Path
     * @param bootstrapFileMappings   Bootstrap Source File to File Id mapping
     */
    public abstract void appendNextPartition(String partitionPath,
        List<BootstrapFileMapping> bootstrapFileMappings);

    /**
     * Writer calls this method after appending all partitions to be indexed.
     */
    public abstract void finish();

    public abstract void close();
  }

  public static BootstrapIndex getBootstrapIndex(HoodieTableMetaClient metaClient) {
    return ((BootstrapIndex) (ReflectionUtils.loadClass(
        BootstrapIndexType.getBootstrapIndexClassName(metaClient.getTableConfig()),
        new Class[] {HoodieTableMetaClient.class}, metaClient)));
  }
}
