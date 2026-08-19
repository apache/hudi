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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.util.Option;

import lombok.extern.slf4j.Slf4j;

/**
 * Classpath detection of engine-specific variant shredding components.
 *
 * <p>Engine modules (currently the Spark 4.x bundles) ship implementations of
 * {@link VariantShreddingProvider} and {@link VariantShreddingSchemaInferrer}; hudi-common
 * discovers them by probing well-known class names so that it stays free of engine
 * dependencies. Probes are memoized: classpath content does not change within a JVM.</p>
 */
@Slf4j
public final class VariantShreddingRuntime {

  /** Provider candidates, most specific first. Mirrors what each Spark bundle ships. */
  private static final String[] PROVIDER_CANDIDATES = {
      "org.apache.hudi.variant.Spark4VariantShreddingProvider"
  };

  /**
   * Inferrer candidates, one per Spark version module that ships one (inference exists only in
   * Spark 4.1+, SPARK-53659), most recent first. Each spark4.x profile builds only its own
   * version module, so every version that should infer needs its own entry here: a runtime whose
   * module is missing from this list silently writes unshredded.
   */
  private static final String[] INFERRER_CANDIDATES = {
      "org.apache.hudi.variant.Spark42VariantShreddingSchemaInferrer",
      "org.apache.hudi.variant.Spark41VariantShreddingSchemaInferrer"
  };

  private static final Option<String> PROVIDER_CLASS = probe(PROVIDER_CANDIDATES);
  private static final Option<VariantShreddingSchemaInferrer> INFERRER = loadInferrer();

  private VariantShreddingRuntime() {
  }

  /**
   * The fully-qualified name of the first {@link VariantShreddingProvider} implementation
   * found on the classpath, if any.
   */
  public static Option<String> getProviderClass() {
    return PROVIDER_CLASS;
  }

  /**
   * A shared {@link VariantShreddingSchemaInferrer} instance from the classpath, if any.
   * Implementations are stateless and thread-safe by contract, so one instance is shared.
   * Tests also use this as the capability probe to filter inference tests to classpaths
   * that ship an inferrer.
   */
  public static Option<VariantShreddingSchemaInferrer> lookupInferrer() {
    return INFERRER;
  }

  /**
   * Both probes run from this class's static initializer, so nothing here may let an error
   * escape: an escaping {@link LinkageError} would fail {@code <clinit>} and every later use
   * (including {@link #getProviderClass()} on the main Avro write path) would see a bare
   * "Could not initialize class" with the original cause lost. Candidates are therefore loaded
   * WITHOUT initialization (no static initializer of theirs runs here), and every
   * {@link LinkageError} degrades to "absent".
   */
  private static Option<String> probe(String[] candidates) {
    for (String candidate : candidates) {
      try {
        Class.forName(candidate, false, VariantShreddingRuntime.class.getClassLoader());
        return Option.of(candidate);
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        // Not on the classpath (or its engine dependencies are absent); try the next candidate.
      } catch (LinkageError e) {
        // Present but unloadable (e.g. class version mismatch): treat as absent rather than fail.
        log.warn("Variant shredding provider {} found on the classpath but failed to load; "
            + "treating it as absent.", candidate, e);
      }
    }
    return Option.empty();
  }

  private static Option<VariantShreddingSchemaInferrer> loadInferrer() {
    for (String candidate : INFERRER_CANDIDATES) {
      try {
        Class<?> clazz = Class.forName(candidate, false, VariantShreddingRuntime.class.getClassLoader());
        return Option.of((VariantShreddingSchemaInferrer) clazz.getDeclaredConstructor().newInstance());
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        // Not on the classpath (or its engine dependencies are absent); try the next candidate.
      } catch (Exception | LinkageError e) {
        // Present but unusable (e.g. linkage failure against an older Spark): degrade to absent.
        log.warn("Variant shredding schema inferrer {} found on the classpath but failed to load; "
            + "shredding schema inference is disabled.", candidate, e);
      }
    }
    return Option.empty();
  }
}
