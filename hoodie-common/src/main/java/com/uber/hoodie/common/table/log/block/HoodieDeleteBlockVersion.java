package com.uber.hoodie.common.table.log.block;

/**
 * A set of feature flags associated with a delete log block format. Versions are changed when the
 * log block format changes. TODO(na) - Implement policies around major/minor versions
 */
final class HoodieDeleteBlockVersion extends HoodieLogBlockVersion {

  HoodieDeleteBlockVersion(int version) {
    super(version);
  }
}
