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

package org.apache.hudi.utils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.junit.jupiter.api.Test;

public class TestBucketUtils {

  @Test
  public void testBucketFileId() {
    for (int i = 0; i < 1000; i++) {
      String bucketId = BucketUtils.bucketIdStr(i);
      String fileId = BucketUtils.newBucketFileIdPrefix(bucketId);
      assert BucketUtils.bucketIdFromFileId(fileId) == i;
    }
  }

  @Test
  public void testBucketId() throws IOException, URISyntaxException {
    File sparkResult = new File(this.getClass().getResource("/hive_bucket_id_check.csv").toURI());
    org.apache.commons.io.FileUtils.readLines(sparkResult)
        .stream().filter(line -> !line.contains("*"))
        .forEach(line -> {
          String[] fields = line.split(",");
          boolean f0 = Boolean.parseBoolean(fields[0]);
          int f1 = Integer.parseInt(fields[1]);
          long f2 = Long.parseLong(fields[2]);
          double f3 = Double.parseDouble(fields[3]);
          String f4 = fields[4];
          int bucketId = Integer.parseInt(fields[5]);
          LinkedList<Object> cols = new LinkedList<>();
          cols.add(f0);
          cols.add(f1);
          cols.add(f2);
          cols.add(f3);
          cols.add(f4);
          assert BucketUtils.bucketId(cols, 100) == bucketId;
        });
  }

}
