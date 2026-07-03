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

package org.apache.hudi.avro;

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
   * Spark 4.1+, SPARK-53659). A new Spark version module adds its implementation here.
   */
  private static final String[] INFERRER_CANDIDATES = {
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

  private static Option<String> probe(String[] candidates) {
    for (String candidate : candidates) {
      try {
        Class.forName(candidate);
        return Option.of(candidate);
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        // Not on the classpath (or its engine dependencies are absent); try the next candidate.
      }
    }
    return Option.empty();
  }

  private static Option<VariantShreddingSchemaInferrer> loadInferrer() {
    for (String candidate : INFERRER_CANDIDATES) {
      try {
        Class<?> clazz = Class.forName(candidate);
        return Option.of((VariantShreddingSchemaInferrer) clazz.getDeclaredConstructor().newInstance());
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        // Not on the classpath; try the next candidate.
      } catch (Throwable t) {
        // Present but unusable (e.g. linkage failure against an older Spark): degrade to absent.
        log.warn("Variant shredding schema inferrer {} found on the classpath but failed to load; "
            + "shredding schema inference is disabled.", candidate, t);
      }
    }
    return Option.empty();
  }
}
