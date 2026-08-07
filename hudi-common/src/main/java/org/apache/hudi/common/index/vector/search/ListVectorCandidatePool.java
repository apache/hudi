/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;

import java.util.List;

/** In-memory retained pool for local execution and engine-neutral tests. */
public final class ListVectorCandidatePool implements VectorCandidatePool {

  private static final long serialVersionUID = 1L;

  private final VectorContinuationController<VectorCandidate> controller;

  public ListVectorCandidatePool(List<VectorCandidate> orderedCandidates, VectorSearchBudget budget) {
    this.controller = new VectorContinuationController<>(orderedCandidates,
        budget.getInitialRerankCandidates(), budget.getRerankBatchSize(), budget.getMaxRerankCandidates());
  }

  @Override
  public boolean hasMore() {
    return controller.hasMore();
  }

  @Override
  public HoodieData<VectorCandidate> nextBatch() {
    return HoodieListData.eager(controller.nextBatch());
  }

  @Override
  public int consumed() {
    return controller.consumed();
  }
}
