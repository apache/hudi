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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class Diagnosis {

  private Diagnosis() {
  }

  public static void waitForFile(Logger log, String basePath, String... segments) {
    Path root = Paths.get(URI.create(basePath));
    Path targetPath = Paths.get(root.toString(), segments);
    try {
      int maxBackoffs = 7;
      int exp = 0;
      while (Files.notExists(targetPath) && exp < maxBackoffs) {
        int waitSeconds = (int) Math.pow(2, exp);
        log.warn(String.format("waiting %s seconds for file %s", waitSeconds, targetPath));
        Thread.sleep(waitSeconds * 1000L);
        exp++;
      }
      if (Files.notExists(targetPath)) {
        log.error(String.format("file does not appear after %s backoffs!", exp));
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static String getDirectoryTree(String basePath, String... excludeExtensions) {
    Path root = Paths.get(URI.create(basePath));
    int indent = 0;
    StringBuilder sb = new StringBuilder();
    sb.append("===> Diagnosis (directory tree): ");
    sb.append(root.toAbsolutePath());
    sb.append("\n");
    try {
      visitDirectory(root, indent, sb, excludeExtensions);
      return sb.toString();
    } catch (IOException e) {
      return Arrays.toString(e.getStackTrace());
    }
  }

  private static void visitDirectory(Path dir, int indent, StringBuilder sb, String... excludeExtensions) throws IOException {
    ValidationUtils.checkArgument(Files.isDirectory(dir), String.format(" %s is not a directory!", dir));
    sb.append(getIndentString(indent));
    sb.append("+--");
    sb.append(dir.toFile().getName());
    sb.append(File.separator);
    sb.append("\n");
    for (Path path : Files.list(dir).sorted().collect(Collectors.toList())) {
      if (Files.isDirectory(path)) {
        visitDirectory(path, indent + 1, sb, excludeExtensions);
      } else {
        visitFile(path, indent + 1, sb, excludeExtensions);
      }
    }
  }

  private static void visitFile(Path path, int indent, StringBuilder sb, String... excludeExtensions) {
    if (Arrays.stream(excludeExtensions).noneMatch(ext -> path.toFile().getName().endsWith(ext))) {
      sb.append(getIndentString(indent));
      sb.append("+--");
      sb.append(path.toFile().getName());
      sb.append("\n");
    }
  }

  private static String getIndentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append("|  ");
    }
    return sb.toString();
  }
}
