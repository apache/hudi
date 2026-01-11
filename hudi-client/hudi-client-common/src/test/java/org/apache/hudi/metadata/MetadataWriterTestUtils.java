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

package org.apache.hudi.metadata;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;

/**
 * Test utility class to access protected methods from HoodieBackedTableMetadataWriter.
 * This class is in the same package to access protected methods without duplication.
 */
public class MetadataWriterTestUtils {

  /**
   * Tag records with location using the metadata writer's tagRecordsWithLocation method.
   * This is a wrapper around the protected method to make it accessible from tests.
   *
   * @param metadataWriter The metadata writer instance
   * @param partitionRecordsMap Map of partition path to records
   * @param isInitializing Whether this is during initialization
   * @return Pair of tagged records and file group IDs
   */
  @SuppressWarnings("rawtypes")
  public static <I, O> Pair<HoodieData<HoodieRecord>, List<HoodieFileGroupId>> tagRecordsWithLocation(
      HoodieBackedTableMetadataWriter<I, O> metadataWriter,
      Map<String, HoodieData<HoodieRecord>> partitionRecordsMap,
      boolean isInitializing) {
    // Access the protected method - this works because we're in the same package
    return metadataWriter.tagRecordsWithLocation(partitionRecordsMap, isInitializing);
  }
}

