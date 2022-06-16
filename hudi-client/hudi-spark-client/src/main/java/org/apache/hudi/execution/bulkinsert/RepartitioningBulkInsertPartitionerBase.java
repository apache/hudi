package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Objects;

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

  protected static class PartitionPathRDDPartitioner extends Partitioner implements Serializable {
    private final SerializableFunctionUnchecked<Object, String> partitionPathExtractor;
    private final int numPartitions;

    PartitionPathRDDPartitioner(SerializableFunctionUnchecked<Object, String> partitionPathExtractor, int numPartitions) {
      this.partitionPathExtractor = partitionPathExtractor;
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int getPartition(Object o) {
      return Math.abs(Objects.hash(partitionPathExtractor.apply(o))) % numPartitions;
    }
  }
}
