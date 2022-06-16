package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.table.BulkInsertPartitioner;

/**
 * Base class for any {@link BulkInsertPartitioner} implementation that does re-partitioning,
 * to better align "logical" (query-engine's partitioning of the incoming dataset) w/ the table's
 * "physical" partitioning
 */
public abstract class RepartitioningBulkInsertPartitionerBase<I> implements BulkInsertPartitioner<I> {

  protected final boolean isPartitionedTable;

  public RepartitioningBulkInsertPartitionerBase(HoodieTableConfig tableConfig) {
    this.isPartitionedTable = tableConfig.getPartitionFields().map(pfs -> pfs.length > 0).orElse(false);
  }

}
