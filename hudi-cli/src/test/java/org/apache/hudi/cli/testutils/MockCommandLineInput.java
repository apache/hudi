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

package org.apache.hudi.cli.testutils;

import org.springframework.shell.Input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface MockCommandLineInput extends Input {
  @Override
  default List<String> words() {
    if (null == rawText() || rawText().isEmpty()) {
      return Collections.emptyList();
    }
    boolean isInQuote = false;
    List<String> result = new ArrayList<>();
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < rawText().length(); i++) {
      char c = rawText().charAt(i);
      if (' ' == c && !isInQuote) {
        if (stringBuilder.length() != 0) {
          result.add(stringBuilder.toString());
          stringBuilder.delete(0, stringBuilder.length());
        }
      } else if ('\'' == c || '"' == c) {
        if (isInQuote) {
          isInQuote = false;
          result.add(stringBuilder.toString());
          stringBuilder.delete(0, stringBuilder.length());
        } else {
          isInQuote = true;
        }
      } else {
        stringBuilder.append(c);
      }
    }
    return result;
  }
}
