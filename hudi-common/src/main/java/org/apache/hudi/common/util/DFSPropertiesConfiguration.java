/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * A simplified versions of Apache commons - PropertiesConfiguration, that supports limited field types and hierarchical
 * configurations within the same folder as the root file.
 *
 * Includes denoted by the same include=filename.properties syntax, with relative path from root file's folder. Lines
 * beginning with '#' are ignored as comments. Final values for properties are resolved by the order in which they are
 * specified in the files, with included files treated as if they are inline.
 *
 * Note: Not reusing commons-configuration since it has too many conflicting runtime deps.
 */
public class DFSPropertiesConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(DFSPropertiesConfiguration.class);

  private final FileSystem fs;

  private final Path rootFile;

  private final TypedProperties props;

  // Keep track of files visited, to detect loops
  private final Set<String> visitedFiles;

  public DFSPropertiesConfiguration(FileSystem fs, Path rootFile, TypedProperties defaults) {
    this.fs = fs;
    this.rootFile = rootFile;
    this.props = defaults;
    this.visitedFiles = new HashSet<>();
    visitFile(rootFile);
  }

  public DFSPropertiesConfiguration(FileSystem fs, Path rootFile) {
    this(fs, rootFile, new TypedProperties());
  }

  public DFSPropertiesConfiguration() {
    this.fs = null;
    this.rootFile = null;
    this.props = new TypedProperties();
    this.visitedFiles = new HashSet<>();
  }

  private String[] splitProperty(String line) {
    int ind = line.indexOf('=');
    String k = line.substring(0, ind).trim();
    String v = line.substring(ind + 1).trim();
    return new String[] {k, v};
  }

  private void visitFile(Path file) {
    try {
      if (visitedFiles.contains(file.getName())) {
        throw new IllegalStateException("Loop detected; file " + file + " already referenced");
      }
      visitedFiles.add(file.getName());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
      addProperties(reader);
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs", ioe);
      throw new IllegalArgumentException("Cannot read properties from dfs", ioe);
    }
  }

  /**
   * Add properties from input stream.
   * 
   * @param reader Buffered Reader
   * @throws IOException
   */
  public void addProperties(BufferedReader reader) throws IOException {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.equals("") || !line.contains("=")) {
          continue;
        }
        String[] split = splitProperty(line);
        if (line.startsWith("include=") || line.startsWith("include =")) {
          visitFile(new Path(rootFile.getParent(), split[1]));
        } else {
          props.setProperty(split[0], split[1]);
        }
      }
    } finally {
      reader.close();
    }
  }

  public TypedProperties getConfig() {
    return props;
  }
}
