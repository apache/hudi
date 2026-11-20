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

package org.apache.hudi.exception;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hudi.exception.ExceptionUtil.throwAsIOExceptionOrRuntimeException;
import static org.apache.hudi.exception.ExceptionUtil.validateErrorMsg;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestExceptionUtil {

  @Test
  void testValidateErrorMsgInNestedCause() {
    IOException rootException = new IOException("File not found: data.parquet");
    IllegalArgumentException middleException = new IllegalArgumentException("Invalid argument", rootException);
    RuntimeException topException = new RuntimeException("Operation failed", middleException);

    // Check top level exception msg
    assertTrue(validateErrorMsg(topException, "File not found"));
    assertTrue(validateErrorMsg(topException, "data.parquet"));
    // Check nested exception msg
    assertTrue(validateErrorMsg(topException, "Invalid argument"));
    assertTrue(validateErrorMsg(topException, "Operation failed"));
    // Validate no exception matches
    assertFalse(validateErrorMsg(topException, "Connection refused"));
  }

  @Test
  void testValidateErrorMsgWithEmptyMessage() {
    RuntimeException exceptionWithMessage = new RuntimeException("Some error");
    assertFalse(validateErrorMsg(exceptionWithMessage, ""));

    RuntimeException exceptionWithoutMessage = new RuntimeException();
    // Empty string should not be found in any message (including null)
    assertFalse(validateErrorMsg(exceptionWithoutMessage, ""));
  }

  @Test
  void causeCycleTerminatesWhenNoExceptionMatches() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);
    assertTrue(ExceptionUtil.isCausedBy(first, IOException.class));
    assertTimeoutPreemptively(Duration.ofSeconds(1),
        () -> assertFalse(ExceptionUtil.isCausedBy(first, IllegalArgumentException.class)));
  }

  @Test
  void getCausesListsTheChainInOrderWithoutSuppressed() {
    IllegalStateException root = new IllegalStateException("root");
    IOException middle = new IOException("middle", root);
    RuntimeException top = new RuntimeException("top", middle);
    top.addSuppressed(new IllegalArgumentException("suppressed"));
    assertEquals(Arrays.asList(top, middle, root), ExceptionUtil.getCauses(top));
    assertSame(root, ExceptionUtil.getRootCause(top));
  }

  @Test
  void getCausesStopsBeforeACycleRepeats() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);
    assertEquals(Arrays.asList(first, second), ExceptionUtil.getCauses(first));
    assertSame(second, ExceptionUtil.getRootCause(first));
  }

  @Test
  void getCausesOfNullIsEmpty() {
    assertEquals(Collections.emptyList(), ExceptionUtil.getCauses(null));
    assertNull(ExceptionUtil.getRootCause(null));
    IOException alone = new IOException("alone");
    assertEquals(Collections.singletonList(alone), ExceptionUtil.getCauses(alone));
    assertSame(alone, ExceptionUtil.getRootCause(alone));
  }

  @Test
  void testThrowAsIOExceptionOrRuntimeException() {
    IOException ioException = new IOException("io");
    IOException thrownIOException = assertThrows(IOException.class, () -> throwAsIOExceptionOrRuntimeException(ioException));
    assertSame(ioException, thrownIOException);

    RuntimeException runtimeException = new RuntimeException("runtime");
    RuntimeException thrownRuntimeException =
        assertThrows(RuntimeException.class, () -> throwAsIOExceptionOrRuntimeException(runtimeException));
    assertSame(runtimeException, thrownRuntimeException);

    Exception checkedException = new Exception("checked");
    IOException wrappedException =
        assertThrows(IOException.class, () -> throwAsIOExceptionOrRuntimeException(checkedException));
    assertSame(checkedException, wrappedException.getCause());
  }
}
