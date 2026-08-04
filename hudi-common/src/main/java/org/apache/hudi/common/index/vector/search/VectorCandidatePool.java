/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.data.HoodieData;

import java.io.Serializable;

/** A single-scan, retained candidate pool consumed in ordered continuation windows. */
public interface VectorCandidatePool extends Serializable {

  boolean hasMore();

  HoodieData<VectorCandidate> nextBatch();

  int consumed();
}
