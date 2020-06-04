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

package org.apache.hudi.common.table.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests HUDI log format version {@link HoodieLogFormatVersion}.
 */
public class TestHoodieLogFormatVersion {
  private static HoodieLogFormatVersion verDefault = 
      new HoodieLogFormatVersion(HoodieLogFormatVersion.DEFAULT_VERSION);
  private static HoodieLogFormatVersion verCurrent = 
      new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION);

  @Test
  public void testHasMagicHeader() {
    assertTrue(verDefault.hasMagicHeader());
    assertTrue(verCurrent.hasMagicHeader());
  }

  @Test
  public void testHasContent() {
    assertTrue(verDefault.hasContent());
    assertTrue(verCurrent.hasContent());
  }

  @Test
  public void testHasContentLength() {
    assertTrue(verDefault.hasContentLength());
    assertTrue(verCurrent.hasContentLength());
  }

  @Test
  public void testHasOrdinal() {
    assertTrue(verDefault.hasOrdinal());
    assertTrue(verCurrent.hasOrdinal());
  }

  @Test
  public void testHasHeader() {
    assertFalse(verDefault.hasHeader());
    assertTrue(verCurrent.hasHeader());
  }

  @Test
  public void testHasFooter() {
    assertFalse(verDefault.hasFooter());
    assertTrue(verCurrent.hasFooter());

    HoodieLogFormatVersion verNew =  
            new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION + 1);
    assertFalse(verNew.hasFooter());
  }

  @Test
  public void testHasLogBlockLength() {
    assertFalse(verDefault.hasLogBlockLength());
    assertTrue(verCurrent.hasLogBlockLength());

    HoodieLogFormatVersion verNew =  
            new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION + 1);
    assertFalse(verNew.hasLogBlockLength());
  }
}
