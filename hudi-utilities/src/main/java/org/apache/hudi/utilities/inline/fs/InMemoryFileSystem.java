package org.apache.hudi.utilities.inline.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * A FileSystem which stores all content in memory and returns as byte[] when {@link #getFileAsBytes()} is called
 * This FileSystem is used only in write path. Does not support any read apis except {@link #getFileAsBytes()}.
 */
public class InMemoryFileSystem extends FileSystem {

  // TODO: this needs to be per path to support num_cores > 1, and we should release the buffer once done
  private ByteArrayOutputStream bos;
  private Configuration conf = null;
  static final String SCHEME = "inmemfs";

  InMemoryFileSystem() {
    bos = new ByteArrayOutputStream();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.conf = conf;
  }

  @Override
  public URI getUri() {
    return URI.create(getScheme());
  }

  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path inlinePath, int bufferSize) {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                   Progressable progressable) throws IOException {
    return new FSDataOutputStream(bos, new Statistics(getScheme()));
  }

  public byte[] getFileAsBytes() {
    return bos.toByteArray();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    throw new UnsupportedOperationException("Can't rename files");
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException("Can't delete files");
  }

  @Override
  public FileStatus[] listStatus(Path inlinePath) throws FileNotFoundException, IOException {
    System.out.println("listStatus invoked ");
    throw new UnsupportedOperationException("No support for listStatus");
    // return new FileStatus[] {getFileStatus(inlinePath)};
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException("Can't set working directory");
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    throw new UnsupportedOperationException("Can't mkdir");
  }

  @Override
  public boolean exists(Path f) throws IOException {
    throw new UnsupportedOperationException("Can't check for exists");
  }

  @Override
  public FileStatus getFileStatus(Path inlinePath) throws IOException {
    throw new UnsupportedOperationException("No support for getFileStatus");
  }

  @Override
  public void close() throws IOException {
    super.close();
    bos.close();
  }
}
