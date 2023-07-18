package org.apache.hudi;

import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class HiddenFileStatus extends FileStatus {
  public HiddenFileStatus(FileStatus f, String secret) throws IOException {
    super(f);
    this.secret = secret;
  }
  public String secret;

  @Override
  public Path getPath() {
    return new HiddenPath(super.getPath(), getSecret());
  }

  public String getSecret() {
    return this.secret;
  }
}
