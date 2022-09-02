package org.apache.hudi.source.flip27;

import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import java.util.Set;

/**
 *
 */
public class HoodieSourceBuilder {

  private Boundedness boundedness;
  private Path path;
  private Configuration conf;
  private Set<String> requiredPartitionPaths;
  private int maxParallism;

  private RowType rowType;

  private MergeOnReadInputFormat mergeOnReadInputFormat;

  public HoodieSourceBuilder boundedness(Boundedness boundedness) {
    this.boundedness = boundedness;
    return this;
  }

  public HoodieSourceBuilder path(Path path) {
    this.path = path;
    return this;
  }

  public HoodieSourceBuilder conf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  public HoodieSourceBuilder requiredPartitionPaths(Set<String> requiredPartitionPaths) {
    this.requiredPartitionPaths = requiredPartitionPaths;
    return this;
  }

  public HoodieSourceBuilder rowType(RowType rowType) {
    this.rowType = rowType;
    return this;
  }

  public HoodieSourceBuilder maxParallism(int maxParallism) {
    this.maxParallism = maxParallism;
    return this;
  }

  public HoodieSourceBuilder mergeOnReadInputFormat(MergeOnReadInputFormat mergeOnReadInputFormat) {
    this.mergeOnReadInputFormat = mergeOnReadInputFormat;
    return this;
  }

  public static HoodieSourceBuilder builder() {
    return new HoodieSourceBuilder();
  }

  public HoodieSource build() {
    return new HoodieSource(boundedness, path, conf, requiredPartitionPaths, maxParallism, rowType, mergeOnReadInputFormat);
  }
}
