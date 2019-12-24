package org.apache.hudi.utilities.inline.fs;

import org.apache.hadoop.fs.Path;

import java.util.UUID;

public class FileSystemTestUtils {

  public static final String TEMP = "tmp";
  public static final String FORWARD_SLASH = "/";
  public static final String FILE_SCHEME = "file";
  public static final String COLON = ":";

  static Path getRandomOuterInMemPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(InMemoryFileSystem.SCHEME + fileSuffix);
  }

  static Path getRandomOuterFSPath() {
    String randomFileName = UUID.randomUUID().toString();
    String fileSuffix = COLON + FORWARD_SLASH + TEMP + FORWARD_SLASH + randomFileName;
    return new Path(FILE_SCHEME + fileSuffix);
  }

  static Path getPhantomFile(Path outerPath, long startOffset, long inlineLength) {
    // Generate phathom inline file
    return InLineFSUtils.getEmbeddedInLineFilePath(outerPath, FILE_SCHEME, startOffset, inlineLength);
  }

}
