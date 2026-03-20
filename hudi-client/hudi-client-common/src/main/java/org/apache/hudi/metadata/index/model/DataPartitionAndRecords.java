/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package org.apache.hudi.metadata.index.model;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Holds the initialization records and data partition if the index is a partitioned index.
 * <p>
 * For non-partitioned indexes, {@code dataPartition} is empty and the records represent the entire index records.
 */
@AllArgsConstructor
@Getter
@Accessors(fluent = true)
public class DataPartitionAndRecords {
  private final int numFileGroups;
  private final Option<String> dataPartition;
  private final HoodieData<HoodieRecord> indexRecords;
}
