package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * NOTE: This class is thread-safe
 */
public class PathCachingFileName extends Path {

  // NOTE: volatile keyword is redundant here and put mostly for reader notice, since all
  //       reads/writes to references are always atomic (including 64-bit JVMs)
  //       https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.7
  private volatile String fileName;

  public PathCachingFileName(URI aUri) {
    super(aUri);
  }

  @Override
  public String getName() {
    // This value could be overwritten concurrently and that's okay, since
    // {@code Path} is immutable
    if (fileName == null) {
      fileName = super.getName();
    }
    return fileName;
  }
}
