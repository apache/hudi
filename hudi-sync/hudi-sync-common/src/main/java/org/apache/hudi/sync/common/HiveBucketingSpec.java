package org.apache.hudi.sync.common;

import java.util.List;

public class HiveBucketingSpec {

  private final int bucketNumber;
  private final List<String> bucketColumns;

  public HiveBucketingSpec(int bucketNumber, List<String> bucketColumns) {
    this.bucketNumber = bucketNumber;
    this.bucketColumns = bucketColumns;
  }

  public int getBucketNumber() {
    return bucketNumber;
  }

  public List<String> getBucketColumns() {
    return bucketColumns;
  }
}
