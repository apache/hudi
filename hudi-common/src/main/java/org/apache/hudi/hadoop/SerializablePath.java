package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * {@link Serializable} wrapper encapsulating {@link Path}
 */
public class SerializablePath implements Serializable {

  private Path path;

  public SerializablePath(Path path) {
    this.path = path;
  }

  public Path get() {
    return path;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeUTF(path.toString());
  }

  private void readObject(ObjectInputStream in) throws IOException {
    String pathStr = in.readUTF();
    path = new CachingPath(pathStr);
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
