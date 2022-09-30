package org.apache.hudi.source.flip27;

import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class HoodieSourceEnumState {

  private final List<String> issuedInstants;
  private final List<MergeOnReadInputSplit> unassigned;

  public HoodieSourceEnumState(List<String> issuedInstants, List<MergeOnReadInputSplit> unassigned) {
    this.issuedInstants = issuedInstants;
    this.unassigned = unassigned;
  }

  public HoodieSourceEnumState() {
    this.unassigned = Collections.emptyList();
    this.issuedInstants = Collections.emptyList();
  }

  public List<String> getIssuedInstants() {
    return issuedInstants;
  }

  public List<MergeOnReadInputSplit> getUnassigned() {
    return unassigned;
  }
}
