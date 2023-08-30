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

package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A custom Avro JSON encoder that avoids wrapping union types in JSON.
 *
 * <p>By default, Avro's JSON encoding for union types is to wrap the union value
 * in a JSON object with the type name as the key (e.g., {"string": "value"}).
 * This encoder overrides that behavior to write just the value, resulting in cleaner JSON output.
 *
 * <p>For instance, a union with schema ["null", "string"] would be encoded as just
 * "value" instead of {"string": "value"}.
 *
 * <p>This encoder is particularly useful when the standard Avro JSON format's verbosity
 * for union types is not desired.
 *
 * <p>Credit for the inspiration behind this implementation goes to:
 * <a href="https://github.com/allegro/json-avro-converter/blob/2973d1546d9e4590f54a029db483d6d8abde808d/converter/src/main/java/org/apache/avro/io/NoWrappingJsonEncoder.java">
 *   allegro/json-avro-converter
 * </a>
 */
public class NoWrappingJsonEncoder extends JsonEncoder {
  public NoWrappingJsonEncoder(Schema sc, OutputStream out) throws IOException {
    super(sc, out);
  }

  /**
   * Overrides the default behavior of JsonEncoder for writing union types.
   * This avoids wrapping the union value in a JSON object, resulting in cleaner JSON output.
   *
   * @param unionIndex the index of the union type to write
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    parser.pushSymbol(symbol);
  }
}
