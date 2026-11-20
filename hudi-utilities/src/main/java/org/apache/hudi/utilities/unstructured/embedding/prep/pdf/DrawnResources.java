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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdfparser.PDFStreamParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The resource names one content stream (a page's /Contents sequence or a form) hands to the operators that make PDFBox
 * text extraction load a resource: {@code Do} (an XObject), {@code Tf} (a font) and {@code gs} (an ExtGState, whose /Font
 * PDFBox loads). PDFStreamEngine resolves each of these by the first operand pending at the operator, so that is the name
 * recorded, read with PDFBox's own {@link PDFStreamParser} so that it is the name PDFBox resolves.
 *
 * <p>More than {@value #MAX_NAMES} distinct names turn the record into "every resource", the guard's behaviour before
 * resources were filtered by use.
 */
final class DrawnResources {

  /** Resource categories whose entries a text extraction loads when an operator names them. */
  enum Category {
    XOBJECT, FONT, EXT_G_STATE
  }

  /** Distinct names kept per stream; past it the record means every resource. */
  static final int MAX_NAMES = 1024;

  /** By COSName, which compares the name's bytes as PDFBox resolves them (a name need not be UTF-8). */
  private final Map<Category, Set<COSName>> names = new EnumMap<>(Category.class);
  private int size;
  private boolean everything;

  /** A record that draws every resource: the fallback when the content cannot be read. */
  static DrawnResources everything() {
    DrawnResources all = new DrawnResources();
    all.everything = true;
    return all;
  }

  /**
   * Reads every token of {@code parser} as PDFStreamEngine does (operands pending since the last operator) and records
   * the first operand of each {@code Do}, {@code Tf} and {@code gs} that is a name.
   */
  static DrawnResources collect(PDFStreamParser parser) throws IOException {
    DrawnResources drawn = new DrawnResources();
    Object first = null;
    boolean pending = false;
    for (Object token = parser.parseNextToken(); token != null; token = parser.parseNextToken()) {
      if (token instanceof Operator) {
        Category category = category(((Operator) token).getName());
        if (category != null && first instanceof COSName) {
          drawn.add(category, (COSName) first);
        }
        first = null;
        pending = false;
      } else if (!pending) {
        first = token;
        pending = true;
      }
    }
    return drawn;
  }

  private static Category category(String operator) {
    switch (operator) {
      case "Do":
        return Category.XOBJECT;
      case "Tf":
        return Category.FONT;
      case "gs":
        return Category.EXT_G_STATE;
      default:
        return null;
    }
  }

  /** Whether the content hands {@code name} of {@code category} to its operator (always, for {@link #everything()}). */
  boolean draws(Category category, COSName name) {
    if (everything) {
      return true;
    }
    Set<COSName> drawn = names.get(category);
    return drawn != null && drawn.contains(name);
  }

  /** The names recorded for {@code category}; empty when none, and meaningless when the record means every resource. */
  Set<COSName> names(Category category) {
    Set<COSName> drawn = names.get(category);
    return drawn == null ? Collections.emptySet() : Collections.unmodifiableSet(drawn);
  }

  boolean drawsEverything() {
    return everything;
  }

  /** Distinct names recorded; 0 once the record means every resource. */
  int size() {
    return everything ? 0 : size;
  }

  void add(Category category, COSName name) {
    if (everything) {
      return;
    }
    if (names.computeIfAbsent(category, c -> new HashSet<>()).add(name) && ++size > MAX_NAMES) {
      everything = true;
      names.clear();
    }
  }
}
