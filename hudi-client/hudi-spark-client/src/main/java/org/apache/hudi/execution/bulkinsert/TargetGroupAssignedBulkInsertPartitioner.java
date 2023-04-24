package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.table.BulkInsertPartitioner;

public abstract class TargetGroupAssignedBulkInsertPartitioner<T> implements BulkInsertPartitioner<T> {

  private final String targetFileGroupId;

  public TargetGroupAssignedBulkInsertPartitioner(final String targetFileGroupId) {
    this.targetFileGroupId = targetFileGroupId;
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    if (StringUtils.isNullOrEmpty(targetFileGroupId)) {
      return BulkInsertPartitioner.super.getFileIdPfx(partitionId);
    } else {
      return targetFileGroupId;
    }
  }
}
