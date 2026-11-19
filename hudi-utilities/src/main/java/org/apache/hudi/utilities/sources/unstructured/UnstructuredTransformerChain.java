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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Composition rules of an unstructured job's required transformer and final schema guard. */
final class UnstructuredTransformerChain {

  private static final String SOURCE_NAME = "UnstructuredDataSource";

  /** The delimiter of a chain entry's {@code <id>:<class>} form ({@link org.apache.hudi.utilities.transform.ChainedTransformer}). */
  private static final String ID_DELIMITER = ":";

  private UnstructuredTransformerChain() {
  }

  static List<String> compose(UnstructuredMode mode, List<String> configured) {
    List<String> chain = composeRequired(mode, configured);
    Class<? extends Transformer> closing = UnstructuredSchemaGuard.class;
    boolean withIds = false;
    Set<String> ids = new HashSet<>();
    for (String entry : chain) {
      Pair<Option<String>, String> name = ChainedTransformer.splitTransformer(entry);
      if (name.getLeft().isPresent()) {
        withIds = true;
        ids.add(name.getLeft().get());
      }
      if (isA(name.getRight(), closing)) {
        throw new HoodieTransformPlanException(SOURCE_NAME + " runs " + closing.getName() + " last itself: remove it from "
            + "--transformer-class. Transformers configured: " + configured);
      }
    }
    String id = closing.getSimpleName();
    if (withIds && ids.contains(id)) {
      throw new HoodieTransformPlanException("--transformer-class uses the id " + id + ", which " + SOURCE_NAME + " gives the "
          + closing.getName() + " it adds last: rename that entry. Transformers configured: " + configured);
    }
    List<String> closed = new ArrayList<>(chain);
    closed.add(withIds ? id + ID_DELIMITER + closing.getName() : closing.getName());
    return closed;
  }

  /**
   * Whether the chain entry {@code entry} ({@code <class>} or {@code <id>:<class>}) names {@code transformer} or a subclass of it.
   */
  public static boolean names(String entry, Class<?> transformer) {
    return isA(ChainedTransformer.splitTransformer(entry).getRight(), transformer);
  }

  /** {@link #compose} before its closing transformer: the required transformer joins the job's entries. */
  private static List<String> composeRequired(UnstructuredMode mode, List<String> configured) {
    Class<? extends Transformer> required = mode.requiredTransformer();
    Set<Class<? extends Transformer>> excludedTransformers = mode.excludedTransformers();
    List<String> entries = configured == null ? Collections.emptyList() : configured;
    int requiredEntries = 0;
    boolean withIds = false;
    Set<String> ids = new HashSet<>();
    List<String> chain = new ArrayList<>(entries.size() + 1);
    for (String entry : entries) {
      Pair<Option<String>, String> name = ChainedTransformer.splitTransformer(entry);
      if (name.getLeft().isPresent()) {
        withIds = true;
        ids.add(name.getLeft().get());
      }
      String className = name.getRight();
      checkNotExcluded(className, excludedTransformers, required, entries);
      if (isA(className, required)) {
        requiredEntries++;
        if (requiredEntries > 1) {
          continue;
        }
      }
      chain.add(entry);
    }
    if (requiredEntries > 0) {
      return requiredEntries == 1 ? configured : chain;
    }
    chain.add(0, requiredEntry(required, withIds, ids, entries));
    return chain;
  }

  /** Refuses a chain entry naming one of the transformers the source does not run with. */
  private static void checkNotExcluded(String className, Set<Class<? extends Transformer>> excludedTransformers,
                                       Class<? extends Transformer> required, List<String> entries) {
    for (Class<? extends Transformer> excluded : excludedTransformers) {
      if (isA(className, excluded)) {
        throw new HoodieTransformPlanException(SOURCE_NAME + " does not run with " + excluded.getName() + ": --transformer-class names "
            + className + ". " + (SOURCE_NAME + " runs " + required.getName() + ". ")
            + "Transformers configured: " + entries);
      }
    }
  }

  /** The entry of the required transformer added first to a chain that has none: with an id when the chain's entries have ids. */
  private static String requiredEntry(Class<? extends Transformer> required, boolean withIds, Set<String> ids, List<String> entries) {
    if (!withIds) {
      return required.getName();
    }
    String id = required.getSimpleName();
    if (ids.contains(id)) {
      throw new HoodieTransformPlanException("--transformer-class uses the id " + id + ", which " + SOURCE_NAME + " gives the "
          + required.getName() + " it adds to the chain: rename that entry, or name " + required.getName() + " in the chain. "
          + "Transformers configured: " + entries);
    }
    return id + ID_DELIMITER + required.getName();
  }

  /**
   * Whether {@code className} is {@code transformer} or a subclass of it. The class is loaded without being initialised, so a
   * class whose static initialiser fails is still recognised; the chain itself instantiates it. A class not on the classpath is
   * not that transformer: the chain reports it when it loads the class.
   */
  private static boolean isA(String className, Class<?> transformer) {
    if (transformer.getName().equals(className)) {
      return true;
    }
    try {
      return transformer.isAssignableFrom(Class.forName(className, false, UnstructuredTransformerChain.class.getClassLoader()));
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
