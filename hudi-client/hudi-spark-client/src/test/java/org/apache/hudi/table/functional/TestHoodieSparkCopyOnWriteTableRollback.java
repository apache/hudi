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

package org.apache.hudi.table.functional;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

@Tag("functional")
public class TestHoodieSparkCopyOnWriteTableRollback extends TestHoodieSparkRollback {

  /**
   * Scenario: data table is updated, no changes to MDT
   */
  @Test
  public void testRollbackWithFailurePreMDT() throws IOException {
    testRollbackWithFailurePreMDT(COPY_ON_WRITE);
  }

  /**
   * Scenario: data table is updated, deltacommit is completed in MDT
   */
  @Test
  public void testRollbackWithFailurePostMDT() throws IOException {
    testRollbackWithFailurePostMDT(COPY_ON_WRITE);
  }

  /**
   * Scenario: data table is updated, deltacommit is completed in MDT then during rollback,
   * data table is updated, no changes to MDT
   */
  @Test
  public void testRollbackWithFailurePostMDTRollbackFailsPreMDT() throws IOException {
    testRollbackWithFailurePostMDT(COPY_ON_WRITE, true);
  }

  /**
   * Scenario: data table is updated, deltacommit of interest is inflight in MDT
   */
  @Test
  public void testRollbackWithFailureInMDT() throws Exception {
    testRollbackWithFailureinMDT(COPY_ON_WRITE);
  }

}
