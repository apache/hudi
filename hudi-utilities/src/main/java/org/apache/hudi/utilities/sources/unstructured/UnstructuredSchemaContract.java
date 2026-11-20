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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The columns an unstructured table must have (its contract), and the checks of a schema against them: a user's target schema,
 * the schema a transformer chain plans, and the rows a round writes. Columns are matched by name; their order is free.
 * <ul>
 *   <li>Every contract column is present, with the same data type ignoring nullability and the same Hudi logical type
 *   ({@code BLOB}, {@code VECTOR(n)}); a non-null contract column may be nullable, a nullable one may not be non-null.</li>
 *   <li>Any other column is an extra: nullable, of any type (a Hudi {@code BLOB} or {@code VECTOR} included; the blob injector
 *   configures only the {@code blob} column, and only the declared models' columns are model columns), and not named like a
 *   contract column, a declared model's column among them, ignoring case, a Hudi meta column or {@code _corrupt_record}.</li>
 * </ul>
 */
public final class UnstructuredSchemaContract {

  /** The column the error table's rows carry their failure in, which the streamer routes to the error table. */
  public static final String CORRUPT_RECORD = "_corrupt_record";

  private UnstructuredSchemaContract() {
  }

  /**
   * The columns {@code mode} writes:
   * <ul>
   *   <li>blob: {@link BlobTableSchema#schema()} with the path partition columns the source adds;</li>
   *   <li>embedding: the columns the round writes ({@link EmbeddingTableSchema#roundSchema}): those of the configuration on a
   *   new table, those of the existing table ({@code tableSchema}, without meta columns) with the active model's VECTOR otherwise.</li>
   * </ul>
   */
  public static StructType contract(UnstructuredMode mode, TypedProperties props, Option<StructType> tableSchema) {
    switch (mode) {
      case BLOB:
        StructType blob = BlobTableSchema.schema();
        for (String column : pathPartitionColumns(props)) {
          if (Arrays.stream(blob.fieldNames()).noneMatch(name -> name.equalsIgnoreCase(column))) {
            blob = blob.add(DataTypes.createStructField(column, DataTypes.StringType, true));
          }
        }
        return blob;
      case EMBEDDING:
        return EmbeddingTableSchema.roundSchema(EmbeddingTableConfig.from(props), tableSchema);
      default:
        throw new UnsupportedOperationException("Unknown unstructured mode " + mode);
    }
  }

  /** The path partition columns the source adds to its rows, as configured. */
  private static List<String> pathPartitionColumns(TypedProperties props) {
    return new ArrayList<>(PathPartitionFields.configured(props));
  }

  /**
   * Checks the user's target schema {@code target} against {@code contract} (C1 at start, C3 at each round).
   *
   * @throws HoodieSchemaException naming the column, what the contract expects and what the schema has, and the source's mode
   */
  public static void checkTarget(UnstructuredMode mode, StructType contract, HoodieSchema target) {
    check(mode, contract, contract, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(target), "the target schema", false, true);
  }

  /**
   * Checks {@code schema}, the schema of a chain's output or of a round's rows, against {@code contract}: the contract columns with
   * their types, and extra columns of any type (a Hudi BLOB or VECTOR included) with no reserved name. The names of
   * {@code newTable}, the contract of a new table of the job's configuration, are reserved too: on an existing table it holds
   * every declared model's column, also one the table does not have yet. Nullability is not compared, since Spark plans and rows
   * carry it loosely; the writer schema's is checked on the target schema. With {@code corruptRecord}, the {@value #CORRUPT_RECORD}
   * column the error table needs must be there too.
   *
   * @throws HoodieSchemaException naming the column and the source's mode; {@code what} names the schema
   */
  public static void checkOutput(UnstructuredMode mode, StructType contract, StructType newTable, StructType schema, String what,
                                 boolean corruptRecord) {
    check(mode, contract, newTable, schema, what, corruptRecord, false);
  }

  /**
   * @param target whether {@code schema} is a target schema: then nullability is compared, extras must be nullable, and
   *               {@value #CORRUPT_RECORD} is a reserved name rather than the error table's column
   */
  private static void check(UnstructuredMode mode, StructType contract, StructType newTable, StructType schema, String what,
                            boolean corruptRecord, boolean target) {
    String source = sourceName(mode);
    for (StructField expected : contract.fields()) {
      StructField found = field(schema, expected.name());
      if (found == null) {
        throw refusal(source, what, expected.name(), describe(expected), "no such column");
      }
      if (!withoutNullability(expected.dataType()).equals(withoutNullability(found.dataType()))
          || !logicalType(expected).equals(logicalType(found))) {
        throw refusal(source, what, expected.name(), describe(expected), describe(found));
      }
      if (target && expected.nullable() && !found.nullable()) {
        throw refusal(source, what, expected.name(), describe(expected) + " nullable", describe(found) + " non-null");
      }
    }
    if (corruptRecord && field(schema, CORRUPT_RECORD) == null) {
      throw new HoodieSchemaException(source + " refuses " + what + ": it has no " + CORRUPT_RECORD + " column, which the error table "
          + "needs; a transformer must keep it");
    }
    for (StructField extra : schema.fields()) {
      boolean errorColumn = !target && CORRUPT_RECORD.equals(extra.name());
      if (field(contract, extra.name()) != null || errorColumn || HoodieRecord.HOODIE_META_COLUMNS.contains(extra.name())) {
        continue;
      }
      checkExtra(source, what, contract, newTable, extra, target);
    }
  }

  /**
   * Plans the job's transformer chain on the source's rows, with {@link Transformer#transformedSchema} and no data (C2), and refuses
   * a chain that would break the contract:
   * <ul>
   *   <li>a transformer before {@code required}: the job's own transformers run only after it;</li>
   *   <li>a transformer after it that drops or retypes a column it is given, {@value #CORRUPT_RECORD} included while the error
   *   table is on (the error rows would be written as good rows);</li>
   *   <li>a planned output that does not meet the contract ({@link #checkOutput}), and, with a user target schema, a written column
   *   beyond the new table's contract that the target schema does not have with the same type.</li>
   * </ul>
   *
   * @param chain the composed entries ({@code <class>} or {@code <id>:<class>}), the mode's required transformer and the closing
   *     {@link UnstructuredSchemaGuard} included
   * @throws HoodieSchemaException naming the transformer or the column, the rule and the source's mode
   */
  public static void checkChainPlan(UnstructuredMode mode, List<String> chain, JavaSparkContext jsc, SparkSession spark, TypedProperties props,
                                    Option<HoodieSchema> userTarget, Option<StructType> tableSchema) {
    // the mode's required transformer runs first; the schema guard is the closing entry
    Class<? extends Transformer> required = mode.requiredTransformer();
    Class<? extends Transformer> closing = UnstructuredSchemaGuard.class;
    String source = sourceName(mode);
    int requiredIndex = requiredIndex(source, required, chain);
    boolean errorTable = ConfigUtils.getBooleanWithAltKeys(props, HoodieErrorTableConfig.ERROR_TABLE_ENABLED);
    // the source's rows are the blob rows of either mode; the error-table chain gives every transformer the error column
    StructType schema = withErrorColumn(contract(UnstructuredMode.BLOB, props, Option.empty()), errorTable);
    Set<String> ids = ids(chain);
    StructType fixed = contract(mode, props, Option.empty());
    ChainPlanner planner = new ChainPlanner(mode, ids, jsc, spark, props);
    schema = planner.plan(chain.get(requiredIndex), schema);
    StructType requiredProduced = producedByRequired(mode, props, tableSchema, fixed, schema);
    List<String> after = chain.subList(requiredIndex + 1, chain.size());
    // the round passes each entry the rows as the previous one left them, a column added again given twice; only the closing
    // guard resolves it, so each entry is planned against the unresolved columns and the add-only rule reads the resolved ones
    for (String entry : after) {
      if (!UnstructuredTransformerChain.names(entry, closing)) {
        StructType unresolved = planner.plan(entry, schema);
        StructType planned = lastWins(mode, fixed, unresolved, "the transformer " + entry);
        StructType given = lastWins(mode, fixed, schema, "the transformer " + entry);
        checkAddOnly(source, required, entry, given, planned);
        schema = unresolved;
      }
    }
    checkPartitionColumns(source, planner, fixed, withErrorColumn(requiredProduced, errorTable), after, closing);
    schema = lastWins(mode, fixed, schema, "the transformer chain");
    StructType contract = contract(mode, props, tableSchema);
    checkOutput(mode, contract, fixed, schema, "the output of the transformer chain", errorTable);
    if (userTarget.isPresent()) {
      checkTargetSchema(source, fixed, schema, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(userTarget.get()));
    }
  }

  /** Keeps required-subclass additions without treating existing embedding-table placeholders as produced columns. */
  private static StructType producedByRequired(UnstructuredMode mode, TypedProperties props, Option<StructType> tableSchema,
                                               StructType fixed, StructType planned) {
    StructType stock = contract(mode, props, tableSchema);
    StructType produced = fixed;
    for (StructField added : planned.fields()) {
      if (!CORRUPT_RECORD.equals(added.name()) && field(fixed, added.name()) == null
          && (mode == UnstructuredMode.BLOB || field(stock, added.name()) == null)) {
        produced = produced.add(added);
      }
    }
    return produced;
  }

  /** Plans one chain entry of the source's mode against a given schema; the arguments every entry of a chain shares. */
  private static final class ChainPlanner {
    private final UnstructuredMode mode;
    private final Set<String> ids;
    private final JavaSparkContext jsc;
    private final SparkSession spark;
    private final TypedProperties props;

    ChainPlanner(UnstructuredMode mode, Set<String> ids, JavaSparkContext jsc, SparkSession spark, TypedProperties props) {
      this.mode = mode;
      this.ids = ids;
      this.jsc = jsc;
      this.spark = spark;
      this.props = props;
    }

    StructType plan(String entry, StructType schema) {
      return UnstructuredSchemaContract.plan(mode, entry, ids, schema, jsc, spark, props);
    }
  }

  /** {@code schema} with the error-table column when the error table is on. */
  private static StructType withErrorColumn(StructType schema, boolean errorTable) {
    return errorTable ? schema.add(DataTypes.createStructField(CORRUPT_RECORD, DataTypes.StringType, true)) : schema;
  }

  /** The index of {@code required} in the chain, which must be its first entry. */
  private static int requiredIndex(String source, Class<? extends Transformer> required, List<String> chain) {
    int requiredIndex = -1;
    for (int i = 0; i < chain.size() && requiredIndex < 0; i++) {
      if (UnstructuredTransformerChain.names(chain.get(i), required)) {
        requiredIndex = i;
      }
    }
    if (requiredIndex < 0) {
      throw new HoodieSchemaException(source + " runs " + required.getName() + " in every round, but the chain has none: " + chain);
    }
    if (requiredIndex > 0) {
      throw new HoodieSchemaException(source + " runs the job's own transformers only after " + required.getName()
          + ": --transformer-class places " + chain.subList(0, requiredIndex) + " before it. Move them after it");
    }
    return requiredIndex;
  }

  /** A transformer after {@code required} keeps every column it is given, with its type: it may only add columns and filter rows. */
  private static void checkAddOnly(String source, Class<? extends Transformer> required, String entry, StructType given,
                                   StructType planned) {
    for (StructField column : given.fields()) {
      StructField kept = field(planned, column.name());
      if (kept == null) {
        throw new HoodieSchemaException(source + " refuses the transformer " + entry + ": it drops the column " + column.name()
            + (CORRUPT_RECORD.equals(column.name()) ? ", which carries the error rows to the error table; they would be written as "
            + "good rows" : "") + ". A transformer after " + required.getSimpleName() + " may only add columns and filter rows");
      }
      if (!withoutNullability(column.dataType()).equals(withoutNullability(kept.dataType())) || !logicalType(column).equals(logicalType(kept))) {
        throw new HoodieSchemaException(source + " refuses the transformer " + entry + ": it changes the column " + column.name()
            + " from " + describe(column) + " to " + describe(kept) + ". A transformer after " + required.getSimpleName()
            + " may only add columns and filter rows");
      }
    }
  }

  /**
   * A configured partition column outside the new table's contract must be produced by the required subclass or later transformers.
   * Replay retains unresolved extra columns until the closing guard's last-wins resolution.
   */
  private static void checkPartitionColumns(String source, ChainPlanner planner, StructType fixed, StructType fixedWithErrors,
                                            List<String> after, Class<? extends Transformer> closing) {
    List<String> partitions = Arrays.stream(ConfigUtils.getStringWithAltKeys(planner.props, KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME, "")
        .split(",")).map(String::trim).filter(column -> !column.isEmpty()).collect(Collectors.toList());
    if (partitions.stream().noneMatch(column -> fieldAt(fixed, column) == null)) {
      return;
    }
    StructType produced = fixedWithErrors;
    for (String entry : after) {
      if (!UnstructuredTransformerChain.names(entry, closing)) {
        produced = planner.plan(entry, produced);
      }
    }
    produced = lastWins(planner.mode, fixed, produced, "the transformer chain");
    for (String column : partitions) {
      if (fieldAt(produced, column) == null) {
        throw new HoodieSchemaException(source + " refuses partition column " + column + " in "
            + KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() + ": this job no longer produces it; configure "
            + CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key() + " or a transformer that produces it. Nothing was written");
      }
    }
  }

  /**
   * The round writes with the user's target schema: every column beyond the new table's contract, a customer's column the table
   * already has or one a transformer overwrites in place among them, must be in it.
   */
  private static void checkTargetSchema(String source, StructType fixed, StructType schema, StructType target) {
    for (StructField added : schema.fields()) {
      if (field(fixed, added.name()) != null || CORRUPT_RECORD.equals(added.name())) {
        continue;
      }
      StructField declared = field(target, added.name());
      if (declared == null || !withoutNullability(declared.dataType()).equals(withoutNullability(added.dataType()))) {
        throw new HoodieSchemaException(source + " refuses the target schema: the round writes the column " + added.name() + " as "
            + describe(added) + " (added by the transformer chain or kept from the table), which the target schema "
            + (declared == null ? "does not have" : "declares as " + describe(declared)) + "; the round writes with the target "
            + "schema, so it must declare every column the round writes");
      }
    }
  }

  /**
   * The schema {@code entry} plans for {@code incoming}, as the streamer's chain plans each transformer (its schema made nullable),
   * with the properties the chain gives that transformer in a round: for an {@code <id>:<class>} entry, the keys suffixed with its
   * id stand for the unsuffixed ones, and the keys of the chain's other ids are left out.
   */
  private static StructType plan(UnstructuredMode mode, String entry, Set<String> ids, StructType incoming, JavaSparkContext jsc,
                                 SparkSession spark, TypedProperties props) {
    String trimmed = entry.trim();
    ChainedTransformer.TransformerInfo info = ChainedTransformer.TransformerInfo.from(entry);
    try (Transformer transformer = info.getTransformer()) {
      TypedProperties transformerProps = info.getProperties(props, ids);
      return transformer.transformedSchema(jsc, spark, incoming, transformerProps).asNullable();
    } catch (HoodieSchemaException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieSchemaException(sourceName(mode) + " cannot plan the transformer " + trimmed + " on the columns "
          + Arrays.toString(incoming.fieldNames()) + ", which the round gives it: " + e.getMessage(), e);
    }
  }

  /** The ids of the chain's {@code <id>:<class>} entries. */
  private static Set<String> ids(List<String> chain) {
    return chain.stream().map(ChainedTransformer::splitTransformer).map(name -> name.getLeft().orElse(null))
        .filter(java.util.Objects::nonNull).collect(Collectors.toSet());
  }

  /**
   * {@code schema} with each name once: of a name given twice, the last column's type at the first column's place, as the round's
   * rows resolve it ({@link UnstructuredSchemaGuard}). A name of the new table's contract (the columns the source and the required
   * transformer make) or {@value #CORRUPT_RECORD} given twice is refused: a customer's transformer may add again only a column the
   * table gained beyond them.
   *
   * @throws HoodieSchemaException naming the column and {@code what}
   */
  static StructType lastWins(UnstructuredMode mode, StructType fixed, StructType schema, String what) {
    Map<String, StructField> last = new LinkedHashMap<>();
    for (StructField field : schema.fields()) {
      if (last.containsKey(field.name()) && (field(fixed, field.name()) != null || CORRUPT_RECORD.equals(field.name()))) {
        throw new HoodieSchemaException(sourceName(mode) + " refuses " + what + ": the column " + field.name() + " is given twice; "
            + "a transformer may not add a column the table's contract fixes");
      }
      last.put(field.name(), field);
    }
    return last.size() == schema.fields().length ? schema : new StructType(last.values().toArray(new StructField[0]));
  }

  /** An extra column: no reserved name, and in a target schema nullable. */
  private static void checkExtra(String source, String what, StructType contract, StructType newTable, StructField extra,
                                 boolean target) {
    String name = extra.name();
    String lower = name.toLowerCase(Locale.ROOT);
    boolean reserved = Arrays.stream(contract.fieldNames()).anyMatch(column -> column.equalsIgnoreCase(name))
        || Arrays.stream(newTable.fieldNames()).anyMatch(column -> column.equalsIgnoreCase(name))
        || lower.startsWith("_hoodie_") || lower.equals(CORRUPT_RECORD);
    if (reserved) {
      throw new HoodieSchemaException(source + " refuses " + what + ": the column " + name + " has a reserved name (a column of the "
          + "table's contract, a declared model's column among them, ignoring case, a Hudi meta column or " + CORRUPT_RECORD + ")");
    }
    if (target && !extra.nullable()) {
      throw new HoodieSchemaException(source + " refuses " + what + ": the extra column " + name + " is non-null; an extra column "
          + "must be nullable, since a round that does not fill it writes null");
    }
  }

  /** The name a refusal names the source by, with its mode. */
  static String sourceName(UnstructuredMode mode) {
    return "UnstructuredDataSource (mode " + mode.configValue() + ")";
  }

  private static HoodieSchemaException refusal(String source, String what, String column, String expected, String found) {
    return new HoodieSchemaException(source + " refuses " + what + ": column " + column + " must be " + expected + ", found: " + found);
  }

  /** The field at a partition path: a top-level name, or a dot-separated path into struct columns; null when absent. */
  private static StructField fieldAt(StructType schema, String path) {
    StructField whole = field(schema, path);
    if (whole != null || !path.contains(".")) {
      return whole;
    }
    StructField found = null;
    StructType current = schema;
    for (String part : path.split("\\.", -1)) {
      found = current == null ? null : field(current, part);
      if (found == null) {
        return null;
      }
      current = found.dataType() instanceof StructType ? (StructType) found.dataType() : null;
    }
    return found;
  }

  private static StructField field(StructType schema, String name) {
    for (StructField field : schema.fields()) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private static String describe(StructField field) {
    String logicalType = logicalType(field);
    return field.dataType().simpleString() + (logicalType.isEmpty() ? "" : " (" + logicalType + ")");
  }

  /** The field's Hudi logical type ({@code BLOB}, {@code VECTOR(n)}), or empty. */
  private static String logicalType(StructField field) {
    Metadata metadata = field.metadata();
    return metadata.contains(HoodieSchema.TYPE_METADATA_FIELD) ? metadata.getString(HoodieSchema.TYPE_METADATA_FIELD) : "";
  }

  /** {@code type} with every nested field, element and value nullable, so two types compare by shape alone. */
  static DataType withoutNullability(DataType type) {
    if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      StructField[] nullable = new StructField[fields.length];
      for (int i = 0; i < fields.length; i++) {
        nullable[i] = new StructField(fields[i].name(), withoutNullability(fields[i].dataType()), true, Metadata.empty());
      }
      return new StructType(nullable);
    }
    if (type instanceof ArrayType) {
      return new ArrayType(withoutNullability(((ArrayType) type).elementType()), true);
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      return new MapType(withoutNullability(map.keyType()), withoutNullability(map.valueType()), true);
    }
    return type;
  }
}
