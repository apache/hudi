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

package org.apache.hudi.table.action.bootstrap;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBootstrapUtils extends HoodieClientTestBase {

  @Test
  public void testAllLeafFoldersWithFiles() throws IOException {
    // All directories including marker dirs.
    List<String> folders = Arrays.asList("2016/04/15", "2016/05/16", "2016/05/17");
    folders.forEach(f -> {
      try {
        metaClient.getFs().mkdirs(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    // Files inside partitions and marker directories
    List<String> files = Stream.of(
        "2016/04/15/1_1-0-1_20190528120000",
        "2016/04/15/2_1-0-1_20190528120000",
        "2016/05/16/3_1-0-1_20190528120000",
        "2016/05/16/4_1-0-1_20190528120000",
        "2016/04/17/5_1-0-1_20190528120000",
        "2016/04/17/6_1-0-1_20190528120000")
        .map(file -> file + metaClient.getTableConfig().getBaseFileFormat().getFileExtension())
        .collect(Collectors.toList());

    files.forEach(f -> {
      try {
        metaClient.getFs().create(new Path(new Path(basePath), f));
      } catch (IOException e) {
        throw new HoodieException(e);
      }
    });

    List<Pair<String, List<HoodieFileStatus>>> collected = BootstrapUtils.getAllLeafFoldersWithFiles(metaClient,
            metaClient.getFs(), basePath, context);
    assertEquals(3, collected.size());
    collected.stream().forEach(k -> {
      assertEquals(2, k.getRight().size());
    });

    // Simulate reading from un-partitioned dataset
    collected = BootstrapUtils.getAllLeafFoldersWithFiles(metaClient, metaClient.getFs(), basePath + "/" + folders.get(0), context);
    assertEquals(1, collected.size());
    collected.stream().forEach(k -> {
      assertEquals(2, k.getRight().size());
    });
  }
}
