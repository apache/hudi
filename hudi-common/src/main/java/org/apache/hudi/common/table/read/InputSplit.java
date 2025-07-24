package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InputSplit {
  private final Option<HoodieBaseFile> baseFileOption;
  private final List<HoodieLogFile> logFiles;
  private final String partitionPath;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;

  InputSplit(Option<HoodieBaseFile> baseFileOption, Stream<HoodieLogFile> logFiles, String partitionPath, long start, long length) {
    this.baseFileOption = baseFileOption;
    this.logFiles = logFiles.sorted(HoodieLogFile.getLogFileComparator())
        .filter(logFile -> !logFile.getFileName().endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
        .collect(Collectors.toList());
    this.partitionPath = partitionPath;
    this.start = start;
    this.length = length;
  }

  static InputSplit fromFileSlice(FileSlice fileSlice, long start, long length) {
    return new InputSplit(fileSlice.getBaseFile(), fileSlice.getLogFiles(), fileSlice.getPartitionPath(),
        start, length);
  }

  public Option<HoodieBaseFile> getBaseFileOption() {
    return baseFileOption;
  }

  public List<HoodieLogFile> getLogFiles() {
    return logFiles;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public long getStart() {
    return start;
  }

  public long getLength() {
    return length;
  }
}
