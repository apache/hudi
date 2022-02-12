package org.apache.hudi.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Abstract base class of the Hive's {@link FileInputFormat} implementations allowing for reading of Hudi's
 * Copy-on-Write (COW) and Merge-on-Read (MOR) tables
 */
public abstract class HoodieTableInputFormat extends FileInputFormat<NullWritable, ArrayWritable>
    implements Configurable {

  protected Configuration conf;

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return super.isSplitable(fs, filename);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
    return super.makeSplit(file, start, length, hosts);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    return super.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    return super.listStatus(job);
  }
}
