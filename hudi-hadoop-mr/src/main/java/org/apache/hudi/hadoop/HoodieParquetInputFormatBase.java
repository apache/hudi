package org.apache.hudi.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.hadoop.realtime.HoodieMergeOnReadTableFileInputFormat;

import java.io.IOException;

/**
 * !!! PLEASE READ CAREFULLY !!!
 *
 * NOTE: Hive bears optimizations which are based upon validating whether {@link FileInputFormat}
 * implementation inherits from {@link MapredParquetInputFormat}.
 *
 * To make sure that Hudi implementations are leveraging these optimizations to the fullest, this class
 * serves as a base-class for every {@link FileInputFormat} implementations working with Parquet file-format.
 *
 * However, this class serves as a simple delegate to the actual implementation hierarchy: it expects
 * either {@link HoodieCopyOnWriteTableFileInputFormat} or {@link HoodieMergeOnReadTableFileInputFormat} to be supplied
 * to which it delegates all of its necessary methods.
 */
public abstract class HoodieParquetInputFormatBase extends MapredParquetInputFormat implements Configurable {

  private final HoodieCopyOnWriteTableFileInputFormat inputFormatDelegate;

  protected HoodieParquetInputFormatBase(HoodieCopyOnWriteTableFileInputFormat inputFormatDelegate) {
    this.inputFormatDelegate = inputFormatDelegate;
  }

  @Override
  public final void setConf(Configuration conf) {
    inputFormatDelegate.setConf(conf);
  }

  @Override
  public final Configuration getConf() {
    return inputFormatDelegate.getConf();
  }

  @Override
  protected final boolean isSplitable(FileSystem fs, Path filename) {
    return inputFormatDelegate.isSplitable(fs, filename);
  }

  @Override
  protected final FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
    return inputFormatDelegate.makeSplit(file, start, length, hosts);
  }

  @Override
  protected final FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts, String[] inMemoryHosts) {
    return inputFormatDelegate.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  @Override
  public final FileStatus[] listStatus(JobConf job) throws IOException {
    return inputFormatDelegate.listStatus(job);
  }
}
