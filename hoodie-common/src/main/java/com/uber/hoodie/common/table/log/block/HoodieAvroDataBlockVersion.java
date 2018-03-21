package com.uber.hoodie.common.table.log.block;

/**
 * A set of feature flags associated with a data log block format. Versions are changed when the log
 * block format changes. TODO(na) - Implement policies around major/minor versions
 */
final class HoodieAvroDataBlockVersion extends HoodieLogBlockVersion {

  HoodieAvroDataBlockVersion(int version) {
    super(version);
  }

  public boolean hasRecordCount() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }
}
