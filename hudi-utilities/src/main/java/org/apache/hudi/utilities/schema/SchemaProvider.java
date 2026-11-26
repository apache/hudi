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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Class to provide schema for reading data and also writing into a Hoodie table,
 * used by deltastreamer (runs over Spark).
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class SchemaProvider implements Serializable {

  // Keyed by "<runtime class>#<provider class>#<getter>"; see overriddenBelow().
  private static final Map<String, Boolean> GETTER_OVERRIDES = new ConcurrentHashMap<>();

  protected TypedProperties config;

  protected JavaSparkContext jssc;

  public SchemaProvider(TypedProperties props) {
    this(props, null);
  }

  protected SchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    this.config = props;
    this.jssc = jssc;
  }

  /**
   * Fetches the source schema from the provider.
   * @return Source schema as a HoodieSchema object.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public HoodieSchema getSourceHoodieSchema() {
    Schema schema = getSourceSchema();
    return schema == null ? null : HoodieSchema.fromAvroSchema(schema);
  }

  /**
   * Fetches the source schema from the provider.
   * @return Source schema as an Avro Schema object.
   * @deprecated since 1.2.0, use {@link #getSourceHoodieSchema()} instead.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  @Deprecated
  public Schema getSourceSchema() {
    throw new UnsupportedOperationException("getSourceSchema() is deprecated and is not implemented for this SchemaProvider. Use getSourceHoodieSchema() instead.");
  }

  /**
   * Fetches the target schema from the provider, defaults to the source schema.
   * @return Target schema as a HoodieSchema object.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public HoodieSchema getTargetHoodieSchema() {
    try {
      // By default, delegate to legacy getTargetSchema() method
      Schema schema = getTargetSchema();
      return schema == null ? null : HoodieSchema.fromAvroSchema(schema);
    } catch (UnsupportedOperationException e) {
      // If the legacy getTargetSchema() calls getSourceSchema() which is not implemented,
      // fall back to using getSourceHoodieSchema as target schema
      return getSourceHoodieSchema();
    }
  }

  /**
   * Fetches the target schema from the provider, defaults to the source schema.
   * @return Target schema as an Avro Schema object.
   * @deprecated since 1.2.0, use {@link #getTargetHoodieSchema()} instead.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.DEPRECATED)
  @Deprecated
  public Schema getTargetSchema() {
    // by default, use source schema as target for hoodie table as well
    return getSourceSchema();
  }

  //every schema provider has the ability to refresh itself, which will mean something different per provider.
  public void refresh() {

  }

  /**
   * Returns the source schema for the {@link #getSourceHoodieSchema()} of a {@code provider} that implements the
   * HoodieSchema getter and keeps the deprecated Avro getter as a bridge.
   *
   * <p>If this provider is a legacy subclass of {@code provider} that overrides only the deprecated
   * {@link #getSourceSchema()}, that override is honored; otherwise {@code schema} supplies the schema. Remove
   * together with the deprecated Avro getters.
   *
   * @param provider the provider class whose getters call this
   * @param schema   supplies the schema of {@code provider} itself
   */
  protected final HoodieSchema resolveSourceHoodieSchema(Class<? extends SchemaProvider> provider,
                                                         Supplier<HoodieSchema> schema) {
    return usesLegacySourceGetter(provider) ? HoodieSchema.fromAvroSchema(getSourceSchema()) : schema.get();
  }

  /**
   * Returns the source schema for the deprecated {@link #getSourceSchema()} bridge of {@code provider}; see
   * {@link #resolveSourceHoodieSchema(Class, Supplier)}.
   *
   * <p>A legacy override reaches this through {@code super}, so it gets {@code schema} directly instead of calling
   * back into itself. Otherwise this goes through {@link #getSourceHoodieSchema()}, so a subclass overriding the
   * HoodieSchema getter is honored by the Avro getter too.
   */
  protected final Schema resolveSourceAvroSchema(Class<? extends SchemaProvider> provider,
                                                 Supplier<HoodieSchema> schema) {
    return toAvroSchema(usesLegacySourceGetter(provider) ? schema.get() : getSourceHoodieSchema());
  }

  /**
   * Target schema counterpart of {@link #resolveSourceHoodieSchema(Class, Supplier)}, for
   * {@link #getTargetHoodieSchema()} and the deprecated {@link #getTargetSchema()}.
   */
  protected final HoodieSchema resolveTargetHoodieSchema(Class<? extends SchemaProvider> provider,
                                                         Supplier<HoodieSchema> schema) {
    return usesLegacyTargetGetter(provider) ? HoodieSchema.fromAvroSchema(getTargetSchema()) : schema.get();
  }

  /**
   * Target schema counterpart of {@link #resolveSourceAvroSchema(Class, Supplier)}.
   */
  protected final Schema resolveTargetAvroSchema(Class<? extends SchemaProvider> provider,
                                                 Supplier<HoodieSchema> schema) {
    return toAvroSchema(usesLegacyTargetGetter(provider) ? schema.get() : getTargetHoodieSchema());
  }

  private boolean usesLegacySourceGetter(Class<? extends SchemaProvider> provider) {
    return overriddenBelow(provider, "getSourceSchema") && !overriddenBelow(provider, "getSourceHoodieSchema");
  }

  private boolean usesLegacyTargetGetter(Class<? extends SchemaProvider> provider) {
    return overriddenBelow(provider, "getTargetSchema") && !overriddenBelow(provider, "getTargetHoodieSchema");
  }

  private static Schema toAvroSchema(HoodieSchema schema) {
    return schema == null ? null : schema.toAvroSchema();
  }

  /**
   * Whether the public no-arg {@code getter} of this provider is declared by a subclass of {@code provider}.
   */
  private boolean overriddenBelow(Class<? extends SchemaProvider> provider, String getter) {
    Class<?> runtimeClass = getClass();
    if (runtimeClass == provider) {
      return false;
    }
    return GETTER_OVERRIDES.computeIfAbsent(runtimeClass.getName() + '#' + provider.getName() + '#' + getter, key -> {
      try {
        Class<?> declaringClass = runtimeClass.getMethod(getter).getDeclaringClass();
        return declaringClass != provider && provider.isAssignableFrom(declaringClass);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException("SchemaProvider has no public method " + getter + "()", e);
      }
    });
  }
}
