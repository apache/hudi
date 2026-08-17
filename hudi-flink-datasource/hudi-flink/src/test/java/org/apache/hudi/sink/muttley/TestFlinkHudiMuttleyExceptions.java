/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.muttley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestFlinkHudiMuttleyExceptions {

  @Test
  void testBaseExceptionConstructorsAndValidation() {
    FlinkHudiMuttleyException withoutMessage = new FlinkHudiMuttleyException(418);
    FlinkHudiMuttleyException withMessage = new FlinkHudiMuttleyException("teapot", 418);

    assertNull(withoutMessage.getMessage());
    assertEquals(418, withoutMessage.getStatusCode());
    assertEquals("teapot", withMessage.getMessage());
    assertEquals(418, FlinkHudiMuttleyException.validate(418, code -> code >= 400));
    assertThrows(IllegalArgumentException.class,
        () -> FlinkHudiMuttleyException.validate(399, code -> code >= 400));
  }

  @Test
  void testClientAndServerExceptionConstructors() {
    FlinkHudiMuttleyClientException client = new FlinkHudiMuttleyClientException(404);
    FlinkHudiMuttleyClientException customClient =
        new FlinkHudiMuttleyClientException("missing", 404);
    FlinkHudiMuttleyServerException server = new FlinkHudiMuttleyServerException(503);
    FlinkHudiMuttleyServerException customServer =
        new FlinkHudiMuttleyServerException("unavailable", 503);

    assertEquals("Muttley client error with status code: 404", client.getMessage());
    assertEquals("missing", customClient.getMessage());
    assertEquals(404, customClient.getStatusCode());
    assertEquals("Muttley server error with status code: 503", server.getMessage());
    assertEquals("unavailable", customServer.getMessage());
    assertEquals(503, customServer.getStatusCode());
  }
}
