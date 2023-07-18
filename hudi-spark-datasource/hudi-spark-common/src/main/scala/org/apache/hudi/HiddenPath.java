package org.apache.hudi;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HiddenPath extends Path {
  public String hiddenString;
  public HiddenPath(Path path, String hiddenString) {
    super(path.getParent(), path.getName());
    this.hiddenString = hiddenString;
  }

}
