package com.uber.hoodie.common.table.log.block;

/**
 * A set of feature flags associated with a command log block format. Versions are changed when the
 * log block format changes. TODO(na) - Implement policies around major/minor versions
 */
final class HoodieCommandBlockVersion extends HoodieLogBlockVersion {

  HoodieCommandBlockVersion(int version) {
    super(version);
  }
}
