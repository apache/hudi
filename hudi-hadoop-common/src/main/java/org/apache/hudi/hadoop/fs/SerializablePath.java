/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

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
    out.writeObject(path.toUri());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    URI uri = (URI) in.readObject();
    path = new CachingPath(uri);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializablePath that = (SerializablePath) o;
    return Objects.equals(path, that.path);
  }
}
