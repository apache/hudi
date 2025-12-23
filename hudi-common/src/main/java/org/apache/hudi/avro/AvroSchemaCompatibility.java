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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Option;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.HoodieAvroUtils.isTypeNumeric;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Evaluate the compatibility between a reader schema and a writer schema. A
 * reader and a writer schema are declared compatible if all datum instances of
 * the writer schema can be successfully decoded using the specified reader
 * schema.
 * <p>
 * NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
 * <p>
 * This code is borrowed from Avro 1.10, with the following modifications:
 * <ol>
 *   <li>Compatibility checks ignore schema name, unless schema is held inside
 *   a union</li>
 * </ol>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class AvroSchemaCompatibility {

  /**
   * Message to annotate reader/writer schema pairs that are compatible.
   */
  public static final String READER_WRITER_COMPATIBLE_MESSAGE = "Reader schema can always successfully decode data written using the writer schema.";

  /**
   * Validates that the provided reader schema can be used to decode avro data
   * written with the provided writer schema.
   *
   * @param reader schema to check.
   * @param writer schema to check.
   * @return a result object identifying any compatibility errors.
   */
  public static SchemaPairCompatibility checkReaderWriterCompatibility(final Schema reader,
                                                                       final Schema writer,
                                                                       boolean checkNamingOverride) {
    final SchemaCompatibilityResult compatibility =
        new ReaderWriterCompatibilityChecker(checkNamingOverride).getCompatibility(reader, writer);

    final String message;
    switch (compatibility.getCompatibility()) {
      case INCOMPATIBLE: {
        message = String.format(
            "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
            writer.toString(true), reader.toString(true));
        break;
      }
      case COMPATIBLE: {
        message = READER_WRITER_COMPATIBLE_MESSAGE;
        break;
      }
      default:
        throw new AvroRuntimeException("Unknown compatibility: " + compatibility);
    }

    return new SchemaPairCompatibility(compatibility, reader, writer, message);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests the equality of two Avro named schemas.
   *
   * <p>
   * Matching includes reader name aliases.
   * </p>
   *
   * @param reader Named reader schema.
   * @param writer Named writer schema.
   * @return whether the names of the named schemas match or not.
   */
  public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
    if (objectsEqual(reader.getName(), writer.getName())) {
      return true;
    }
    // Apply reader aliases:
    return reader.getAliases().contains(writer.getFullName());
  }

  /**
   * Identifies the writer field that corresponds to the specified reader field.
   *
   * <p>
   * Matching includes reader name aliases.
   * </p>
   *
   * @param writerSchema Schema of the record where to look for the writer field.
   * @param readerField  Reader field to identify the corresponding writer field
   *                     of.
   * @return the writer field, if any does correspond, or None.
   */
  public static Field lookupWriterField(final Schema writerSchema, final Field readerField) {
    assert (writerSchema.getType() == Type.RECORD);
    final List<Field> writerFields = new ArrayList<>();
    final Field direct = writerSchema.getField(readerField.name());
    if (direct != null) {
      writerFields.add(direct);
    }
    for (final String readerFieldAliasName : readerField.aliases()) {
      final Field writerField = writerSchema.getField(readerFieldAliasName);
      if (writerField != null) {
        writerFields.add(writerField);
      }
    }
    switch (writerFields.size()) {
      case 0:
        return null;
      case 1:
        return writerFields.get(0);
      default: {
        throw new AvroRuntimeException(String.format(
            "Reader record field %s matches multiple fields in writer record schema %s", readerField, writerSchema));
      }
    }
  }

  /**
   * Reader/writer schema pair that can be used as a key in a hash map.
   * <p>
   * This reader/writer pair differentiates Schema objects based on their system
   * hash code.
   */
  private static final class ReaderWriter {
    private final Schema mReader;
    private final Schema mWriter;

    /**
     * Initializes a new reader/writer pair.
     *
     * @param reader Reader schema.
     * @param writer Writer schema.
     */
    public ReaderWriter(final Schema reader, final Schema writer) {
      mReader = reader;
      mWriter = writer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return System.identityHashCode(mReader) ^ System.identityHashCode(mWriter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ReaderWriter)) {
        return false;
      }
      final ReaderWriter that = (ReaderWriter) obj;
      // Use pointer comparison here:
      return (this.mReader == that.mReader) && (this.mWriter == that.mWriter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return String.format("ReaderWriter{reader:%s, writer:%s}", mReader, mWriter);
    }
  }

  /**
   * Determines the compatibility of a reader/writer schema pair.
   *
   * <p>
   * Provides memoization to handle recursive schemas.
   * </p>
   */
  private static final class ReaderWriterCompatibilityChecker {
    private final AvroDefaultValueAccessor defaultValueAccessor = new AvroDefaultValueAccessor();
    private final Map<ReaderWriter, SchemaCompatibilityResult> mMemoizeMap = new HashMap<>();
    private final boolean checkNaming;

    public ReaderWriterCompatibilityChecker(boolean checkNaming) {
      this.checkNaming = checkNaming;
    }

    /**
     * Reports the compatibility of a reader/writer schema pair.
     *
     * <p>
     * Memorizes the compatibility results.
     * </p>
     *
     * @param reader Reader schema to test.
     * @param writer Writer schema to test.
     * @return the compatibility of the reader/writer schema pair.
     */
    public SchemaCompatibilityResult getCompatibility(final Schema reader, final Schema writer) {
      ArrayDeque<LocationInfo> locations = new ArrayDeque<>(
          Collections.singletonList(new LocationInfo(reader.getName(), reader.getType()))
      );
      return getCompatibility(reader, writer, locations);
    }

    /**
     * Reports the compatibility of a reader/writer schema pair.
     * <p>
     * Memorizes the compatibility results.
     * </p>
     *
     * @param reader    Reader schema to test.
     * @param writer    Writer schema to test.
     * @param locations Stack tracking the path (chain of locations) within the
     *                  schema.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityResult getCompatibility(final Schema reader,
                                                       final Schema writer,
                                                       final Deque<LocationInfo> locations) {
      log.debug("Checking compatibility of reader {} with writer {}", reader, writer);
      final ReaderWriter pair = new ReaderWriter(reader, writer);
      SchemaCompatibilityResult result = mMemoizeMap.get(pair);
      if (result != null) {
        if (result.getCompatibility() == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
          // Break the recursion here.
          // schemas are compatible unless proven incompatible:
          result = SchemaCompatibilityResult.compatible();
        }
      } else {
        // Mark this reader/writer pair as "in progress":
        mMemoizeMap.put(pair, SchemaCompatibilityResult.recursionInProgress());
        result = calculateCompatibility(reader, writer, locations);
        mMemoizeMap.put(pair, result);
      }
      return result;
    }

    private static String getLocationName(final Deque<LocationInfo> locations, Type readerType) {
      StringBuilder sb = new StringBuilder();
      Iterator<LocationInfo> locationInfoIterator = locations.iterator();
      boolean addDot = false;
      while (locationInfoIterator.hasNext()) {
        if (addDot) {
          sb.append(".");
        } else {
          addDot = true;
        }
        LocationInfo next = locationInfoIterator.next();
        sb.append(next.name);
        //we check the reader type if we are at the last location. This is because
        //if the type is array/map, that means the problem is that the field type
        //of the writer is not array/map. If the type is something else, the problem
        //is between the array element/map value of the reader and writer schemas
        if (next.type.equals(Type.MAP)) {
          if (locationInfoIterator.hasNext() || !readerType.equals(Type.MAP)) {
            sb.append(".value");
          }
        } else if (next.type.equals(Type.ARRAY)) {
          if (locationInfoIterator.hasNext() || !readerType.equals(Type.ARRAY)) {
            sb.append(".element");
          }
        }
      }
      return sb.toString();
    }

    /**
     * Calculates the compatibility of a reader/writer schema pair.
     *
     * <p>
     * Relies on external memoization performed by
     * {@link #getCompatibility(Schema, Schema)}.
     * </p>
     *
     * @param reader    Reader schema to test.
     * @param writer    Writer schema to test.
     * @param locations Stack with which to track the location within the schema.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityResult calculateCompatibility(final Schema reader, final Schema writer,
                                                             final Deque<LocationInfo> locations) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();

      if (reader.getType() == writer.getType()) {
        switch (reader.getType()) {
          case NULL:
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case BYTES:
          case STRING: {
            return result;
          }
          case ARRAY: {
            return result.mergedWith(getCompatibility(reader.getElementType(), writer.getElementType(), locations));
          }
          case MAP: {
            return result.mergedWith(getCompatibility(reader.getValueType(), writer.getValueType(), locations));
          }
          case FIXED: {
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkFixedSize(reader, writer, locations));
          }
          case ENUM: {
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkReaderEnumContainsAllWriterEnumSymbols(reader, writer, locations));
          }
          case RECORD: {
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkReaderWriterRecordFields(reader, writer, locations));
          }
          case UNION: {
            // Check that each individual branch of the writer union can be decoded:
            for (final Schema writerBranch : writer.getTypes()) {
              SchemaCompatibilityResult compatibility = getCompatibility(reader, writerBranch, locations);
              if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
                String message = String.format("reader union lacking writer type: %s for field: '%s'", writerBranch.getType(), getLocationName(locations, reader.getType()));
                result = result.mergedWith(SchemaCompatibilityResult.incompatible(
                    SchemaIncompatibilityType.MISSING_UNION_BRANCH, reader, writer, message, asList(locations)));
              }
            }
            // Each schema in the writer union can be decoded with the reader:
            return result;
          }

          default: {
            throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
          }
        }

      } else {
        // Reader and writer have different schema types:

        // Reader compatible with all branches of a writer union is compatible
        if (writer.getType() == Schema.Type.UNION) {
          for (Schema s : writer.getTypes()) {
            result = result.mergedWith(getCompatibility(reader, s, locations));
          }
          return result;
        }

        switch (reader.getType()) {
          case NULL:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case BOOLEAN:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case INT:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case LONG: {
            return (writer.getType() == Type.INT) ? result : result.mergedWith(typeMismatch(reader, writer, locations));
          }
          case FLOAT: {
            return ((writer.getType() == Type.INT) || (writer.getType() == Type.LONG)) ? result
                : result.mergedWith(typeMismatch(reader, writer, locations));

          }
          case DOUBLE: {
            return ((writer.getType() == Type.INT) || (writer.getType() == Type.LONG) || (writer.getType() == Type.FLOAT))
                ? result
                : result.mergedWith(typeMismatch(reader, writer, locations));
          }
          case BYTES: {
            return (writer.getType() == Type.STRING) ? result : result.mergedWith(typeMismatch(reader, writer, locations));
          }
          case STRING: {
            return (isTypeNumeric(writer.getType()) || (writer.getType() == Schema.Type.BYTES)
                ? result : result.mergedWith(typeMismatch(reader, writer, locations)));
          }

          case ARRAY:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case MAP:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case FIXED:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case ENUM:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case RECORD:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case UNION: {
            for (final Schema readerBranch : reader.getTypes()) {
              SchemaCompatibilityResult compatibility = getCompatibility(readerBranch, writer, locations);
              if (compatibility.getCompatibility() == SchemaCompatibilityType.COMPATIBLE) {
                return result;
              }
            }
            // No branch in the reader union has been found compatible with the writer
            // schema:
            String message = String.format("reader union lacking writer type: %s for field: '%s'", writer.getType(), getLocationName(locations, reader.getType()));
            return result.mergedWith(SchemaCompatibilityResult
                .incompatible(SchemaIncompatibilityType.MISSING_UNION_BRANCH, reader, writer, message, asList(locations)));
          }

          default: {
            throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
          }
        }
      }
    }

    private SchemaCompatibilityResult checkReaderWriterRecordFields(final Schema reader, final Schema writer,
                                                                    final Deque<LocationInfo> locations) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();

      // Check that each field in the reader record can be populated from the writer
      // record:
      for (final Field readerField : reader.getFields()) {
        final Field writerField = lookupWriterField(writer, readerField);
        if (writerField == null) {
          // Reader field does not correspond to any field in the writer record schema, so
          // the
          // reader field must have a default value.
          if (defaultValueAccessor.getDefaultValue(readerField) == null) {
            // reader field has no default value
            String message = String.format("Field '%s.%s' has no default value", getLocationName(locations, readerField.schema().getType()), readerField.name());
            result = result.mergedWith(
                SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE,
                    reader, writer, message, asList(locations)));
          }
        } else {
          locations.addLast(new LocationInfo(readerField.name(), readerField.schema().getType()));
          result = result.mergedWith(getCompatibility(readerField.schema(), writerField.schema(), locations));
          locations.removeLast();
        }
      }

      return result;
    }

    private static class AvroDefaultValueAccessor {
      // Avro <= 1.8.2
      private final Option<Method> legacyDefaultValueMethod = loadMethodNoThrow("defaultValue");
      // Avro >= 1.10.0
      private final Option<Method> newDefaultValueMethod = loadMethodNoThrow("defaultVal");

      public Object getDefaultValue(Field field) {
        return newDefaultValueMethod.or(legacyDefaultValueMethod)
            .map(m -> invokeMethodNoThrow(m, field).asLeft())
            .orElse(null);
      }

      private static Either<Object, Exception> invokeMethodNoThrow(Method m, Object obj, Object... args) {
        try {
          return Either.left(m.invoke(obj, args));
        } catch (IllegalAccessException | InvocationTargetException e) {
          return Either.right(e);
        }
      }

      private static Option<Method> loadMethodNoThrow(String defaultValue) {
        try {
          return Option.of(Field.class.getMethod(defaultValue));
        } catch (NoSuchMethodException e) {
          return Option.empty();
        }
      }
    }

    private SchemaCompatibilityResult checkReaderEnumContainsAllWriterEnumSymbols(final Schema reader,
                                                                                  final Schema writer, final Deque<LocationInfo> locations) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      final Set<String> symbols = new TreeSet<>(writer.getEnumSymbols());
      symbols.removeAll(reader.getEnumSymbols());
      if (!symbols.isEmpty()) {
        String message = String.format("Field '%s' missing enum symbols: %s", getLocationName(locations, reader.getType()), symbols);
        result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, reader,
            writer, message, asList(locations));
      }
      return result;
    }

    private SchemaCompatibilityResult checkFixedSize(final Schema reader, final Schema writer,
                                                     final Deque<LocationInfo> locations) {
      int actual = reader.getFixedSize();
      int expected = writer.getFixedSize();
      if (actual != expected) {
        String message = String.format("Fixed size field '%s' expected: %d, found: %d", getLocationName(locations, reader.getType()), expected, actual);
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH, reader, writer,
            message, asList(locations));
      }
      return checkDecimalWidening(reader, writer, locations);
    }

    private SchemaCompatibilityResult checkDecimalWidening(final Schema reader, final Schema writer,
                                                           final Deque<LocationInfo> locations) {
      boolean isReaderDecimal = reader.getLogicalType() instanceof LogicalTypes.Decimal;
      boolean isWriterDecimal = writer.getLogicalType() instanceof LogicalTypes.Decimal;
      if (!isReaderDecimal && !isWriterDecimal) {
        return SchemaCompatibilityResult.compatible();
      }

      if (!isReaderDecimal || !isWriterDecimal) {
        String message = String.format("Decimal field '%s' expected: %s, found: %s", getLocationName(locations, reader.getType()), writer, reader);
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.DECIMAL_MISMATCH, reader, writer,
            message, asList(locations));
      }

      int readerScale = ((LogicalTypes.Decimal) reader.getLogicalType()).getScale();
      int writerScale = ((LogicalTypes.Decimal) writer.getLogicalType()).getScale();
      int readerPrecision = ((LogicalTypes.Decimal) reader.getLogicalType()).getPrecision();
      int writerPrecision = ((LogicalTypes.Decimal) writer.getLogicalType()).getPrecision();
      if (readerScale == writerScale && readerPrecision == writerPrecision) {
        return SchemaCompatibilityResult.compatible();
      }

      if (((readerPrecision - readerScale) < (writerPrecision - writerScale)) || (readerScale < writerScale)) {
        String message = String.format("Decimal field '%s' evolution is lossy. Existing precision: %d, scale: %d, Incoming precision: %d, scale: %d",
            getLocationName(locations, reader.getType()), writerPrecision, writerScale, readerPrecision, readerScale);
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.DECIMAL_MISMATCH, reader, writer,
            message, asList(locations));
      }

      return SchemaCompatibilityResult.compatible();
    }

    private SchemaCompatibilityResult checkSchemaNames(final Schema reader, final Schema writer,
                                                       final Deque<LocationInfo> locations) {
      checkState(locations.size() > 0);
      // NOTE: We're only going to validate schema names in following cases
      //          - This is a top-level schema (ie enclosing one)
      //          - This is a schema enclosed w/in a union (since in that case schemas could be
      //          reverse-looked up by their fully-qualified names)
      boolean shouldCheckNames = checkNaming && (locations.size() == 1 || locations.peekLast().type == Type.UNION);
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      if (shouldCheckNames && !Objects.equals(reader.getFullName(), writer.getFullName())) {
        String message = String.format("Reader schema name: '%s' is not compatible with writer schema name: '%s'", reader.getFullName(), writer.getFullName());
        result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.NAME_MISMATCH, reader, writer,
            message, asList(locations));
      }
      return result;
    }

    private SchemaCompatibilityResult typeMismatch(final Schema reader, final Schema writer,
                                                   final Deque<LocationInfo> locations) {
      String message = String.format("reader type '%s' not compatible with writer type '%s' for field '%s'", reader.getType(),
          writer.getType(), getLocationName(locations, reader.getType()));
      return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH, reader, writer, message,
          asList(locations));
    }

    public static class LocationInfo {
      private final String name;
      private final Type type;

      public LocationInfo(String name, Type type) {
        this.name = name;
        this.type = type;
      }

      @Override
      public String toString() {
        return String.format("%s:%s", name, type);
      }
    }

    private static List<String> asList(Deque<LocationInfo> deque) {
      List<String> list = deque.stream().map(locInfo -> locInfo.name).collect(Collectors.toList());
      return Collections.unmodifiableList(list);
    }
  }

  /**
   * Identifies the type of schema compatibility result.
   */
  public enum SchemaCompatibilityType {
    COMPATIBLE, INCOMPATIBLE,

    /**
     * Used internally to tag a reader/writer schema pair and prevent recursion.
     */
    RECURSION_IN_PROGRESS
  }

  public enum SchemaIncompatibilityType {
    NAME_MISMATCH, FIXED_SIZE_MISMATCH, MISSING_ENUM_SYMBOLS, READER_FIELD_MISSING_DEFAULT_VALUE, TYPE_MISMATCH,
    DECIMAL_MISMATCH, MISSING_UNION_BRANCH
  }

  /**
   * Immutable class representing details about a particular schema pair
   * compatibility check.
   */
  public static final class SchemaCompatibilityResult {

    /**
     * Merges the current {@code SchemaCompatibilityResult} with the supplied result
     * into a new instance, combining the list of
     * {@code Incompatibility Incompatibilities} and regressing to the
     * {@code SchemaCompatibilityType#INCOMPATIBLE INCOMPATIBLE} state if any
     * incompatibilities are encountered.
     *
     * @param toMerge The {@code SchemaCompatibilityResult} to merge with the
     *                current instance.
     * @return A {@code SchemaCompatibilityResult} that combines the state of the
     * current and supplied instances.
     */
    public SchemaCompatibilityResult mergedWith(SchemaCompatibilityResult toMerge) {
      List<Incompatibility> mergedIncompatibilities = new ArrayList<>(mIncompatibilities);
      mergedIncompatibilities.addAll(toMerge.getIncompatibilities());
      SchemaCompatibilityType compatibilityType = mCompatibilityType == SchemaCompatibilityType.COMPATIBLE
          ? toMerge.mCompatibilityType
          : SchemaCompatibilityType.INCOMPATIBLE;
      return new SchemaCompatibilityResult(compatibilityType, mergedIncompatibilities);
    }

    private final SchemaCompatibilityType mCompatibilityType;
    // the below fields are only valid if INCOMPATIBLE
    private final List<Incompatibility> mIncompatibilities;
    // cached objects for stateless details
    private static final SchemaCompatibilityResult COMPATIBLE = new SchemaCompatibilityResult(
        SchemaCompatibilityType.COMPATIBLE, Collections.emptyList());
    private static final SchemaCompatibilityResult RECURSION_IN_PROGRESS = new SchemaCompatibilityResult(
        SchemaCompatibilityType.RECURSION_IN_PROGRESS, Collections.emptyList());

    private SchemaCompatibilityResult(SchemaCompatibilityType compatibilityType,
                                      List<Incompatibility> incompatibilities) {
      this.mCompatibilityType = compatibilityType;
      this.mIncompatibilities = incompatibilities;
    }

    /**
     * Returns a details object representing a compatible schema pair.
     *
     * @return a SchemaCompatibilityDetails object with COMPATIBLE
     * SchemaCompatibilityType, and no other state.
     */
    public static SchemaCompatibilityResult compatible() {
      return COMPATIBLE;
    }

    /**
     * Returns a details object representing a state indicating that recursion is in
     * progress.
     *
     * @return a SchemaCompatibilityDetails object with RECURSION_IN_PROGRESS
     * SchemaCompatibilityType, and no other state.
     */
    public static SchemaCompatibilityResult recursionInProgress() {
      return RECURSION_IN_PROGRESS;
    }

    /**
     * Returns a details object representing an incompatible schema pair, including
     * error details.
     *
     * @return a SchemaCompatibilityDetails object with INCOMPATIBLE
     * SchemaCompatibilityType, and state representing the violating part.
     */
    public static SchemaCompatibilityResult incompatible(SchemaIncompatibilityType incompatibilityType,
                                                         Schema readerFragment, Schema writerFragment, String message, List<String> location) {
      Incompatibility incompatibility = new Incompatibility(incompatibilityType, readerFragment, writerFragment,
          message, location);
      return new SchemaCompatibilityResult(SchemaCompatibilityType.INCOMPATIBLE,
          Collections.singletonList(incompatibility));
    }

    /**
     * Returns the SchemaCompatibilityType, always non-null.
     *
     * @return a SchemaCompatibilityType instance, always non-null
     */
    public SchemaCompatibilityType getCompatibility() {
      return mCompatibilityType;
    }

    /**
     * If the compatibility is INCOMPATIBLE, returns {@link Incompatibility
     * Incompatibilities} found, otherwise an empty list.
     *
     * @return a list of {@link Incompatibility Incompatibilities}, may be empty,
     * never null.
     */
    public List<Incompatibility> getIncompatibilities() {
      return mIncompatibilities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((mCompatibilityType == null) ? 0 : mCompatibilityType.hashCode());
      result = prime * result + ((mIncompatibilities == null) ? 0 : mIncompatibilities.hashCode());
      return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      SchemaCompatibilityResult other = (SchemaCompatibilityResult) obj;
      if (mIncompatibilities == null) {
        if (other.mIncompatibilities != null) {
          return false;
        }
      } else if (!mIncompatibilities.equals(other.mIncompatibilities)) {
        return false;
      }
      return mCompatibilityType == other.mCompatibilityType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return String.format("SchemaCompatibilityResult{compatibility:%s, incompatibilities:%s}", mCompatibilityType,
          mIncompatibilities);
    }
  }

  // -----------------------------------------------------------------------------------------------

  public static final class Incompatibility {
    private final SchemaIncompatibilityType mType;
    private final Schema mReaderFragment;
    private final Schema mWriterFragment;
    private final String mMessage;
    private final List<String> mLocation;

    Incompatibility(SchemaIncompatibilityType type, Schema readerFragment, Schema writerFragment, String message,
                    List<String> location) {
      super();
      this.mType = type;
      this.mReaderFragment = readerFragment;
      this.mWriterFragment = writerFragment;
      this.mMessage = message;
      this.mLocation = location;
    }

    /**
     * Returns the SchemaIncompatibilityType.
     *
     * @return a SchemaIncompatibilityType instance.
     */
    public SchemaIncompatibilityType getType() {
      return mType;
    }

    /**
     * Returns the fragment of the reader schema that failed compatibility check.
     *
     * @return a Schema instance (fragment of the reader schema).
     */
    public Schema getReaderFragment() {
      return mReaderFragment;
    }

    /**
     * Returns the fragment of the writer schema that failed compatibility check.
     *
     * @return a Schema instance (fragment of the writer schema).
     */
    public Schema getWriterFragment() {
      return mWriterFragment;
    }

    /**
     * Returns a human-readable message with more details about what failed. Syntax
     * depends on the SchemaIncompatibilityType.
     *
     * @return a String with details about the incompatibility.
     * @see #getType()
     */
    public String getMessage() {
      return mMessage;
    }

    /**
     * Returns a
     * <a href="https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-08">JSON
     * Pointer</a> describing the node location within the schema's JSON document
     * tree where the incompatibility was encountered.
     *
     * @return JSON Pointer encoded as a string.
     */
    public String getLocation() {
      StringBuilder s = new StringBuilder("/");
      boolean first = true;
      // ignore root element
      for (String coordinate : mLocation.subList(1, mLocation.size())) {
        if (first) {
          first = false;
        } else {
          s.append('/');
        }
        // Apply JSON pointer escaping.
        s.append(coordinate.replace("~", "~0").replace("/", "~1"));
      }
      return s.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((mType == null) ? 0 : mType.hashCode());
      result = prime * result + ((mReaderFragment == null) ? 0 : mReaderFragment.hashCode());
      result = prime * result + ((mWriterFragment == null) ? 0 : mWriterFragment.hashCode());
      result = prime * result + ((mMessage == null) ? 0 : mMessage.hashCode());
      result = prime * result + ((mLocation == null) ? 0 : mLocation.hashCode());
      return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Incompatibility other = (Incompatibility) obj;
      if (mType != other.mType) {
        return false;
      }
      if (mReaderFragment == null) {
        if (other.mReaderFragment != null) {
          return false;
        }
      } else if (!mReaderFragment.equals(other.mReaderFragment)) {
        return false;
      }
      if (mWriterFragment == null) {
        if (other.mWriterFragment != null) {
          return false;
        }
      } else if (!mWriterFragment.equals(other.mWriterFragment)) {
        return false;
      }
      if (mMessage == null) {
        if (other.mMessage != null) {
          return false;
        }
      } else if (!mMessage.equals(other.mMessage)) {
        return false;
      }
      if (mLocation == null) {
        return other.mLocation == null;
      } else {
        return mLocation.equals(other.mLocation);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return String.format("Incompatibility{type:%s, location:%s, message:%s, reader:%s, writer:%s}", mType,
          getLocation(), mMessage, mReaderFragment, mWriterFragment);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Provides information about the compatibility of a single reader and writer
   * schema pair.
   * <p>
   * Note: This class represents a one-way relationship from the reader to the
   * writer schema.
   */
  public static final class SchemaPairCompatibility {
    /**
     * The details of this result.
     */
    private final SchemaCompatibilityResult mResult;

    /**
     * Validated reader schema.
     */
    private final Schema mReader;

    /**
     * Validated writer schema.
     */
    private final Schema mWriter;

    /**
     * Human-readable description of this result.
     */
    private final String mDescription;

    /**
     * Constructs a new instance.
     *
     * @param result      The result of the compatibility check.
     * @param reader      schema that was validated.
     * @param writer      schema that was validated.
     * @param description of this compatibility result.
     */
    public SchemaPairCompatibility(SchemaCompatibilityResult result, Schema reader, Schema writer, String description) {
      mResult = result;
      mReader = reader;
      mWriter = writer;
      mDescription = description;
    }

    /**
     * Gets the type of this result.
     *
     * @return the type of this result.
     */
    public SchemaCompatibilityType getType() {
      return mResult.getCompatibility();
    }

    /**
     * Gets more details about the compatibility, in particular if getType() is
     * INCOMPATIBLE.
     *
     * @return the details of this compatibility check.
     */
    public SchemaCompatibilityResult getResult() {
      return mResult;
    }

    /**
     * Gets the reader schema that was validated.
     *
     * @return reader schema that was validated.
     */
    public Schema getReader() {
      return mReader;
    }

    /**
     * Gets the writer schema that was validated.
     *
     * @return writer schema that was validated.
     */
    public Schema getWriter() {
      return mWriter;
    }

    /**
     * Gets a human-readable description of this validation result.
     *
     * @return a human-readable description of this validation result.
     */
    public String getDescription() {
      return mDescription;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      return String.format("SchemaPairCompatibility{result:%s, readerSchema:%s, writerSchema:%s, description:%s}",
          mResult, mReader, mWriter, mDescription);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
      if ((other instanceof SchemaPairCompatibility)) {
        final SchemaPairCompatibility result = (SchemaPairCompatibility) other;
        return objectsEqual(result.mResult, mResult) && objectsEqual(result.mReader, mReader)
            && objectsEqual(result.mWriter, mWriter) && objectsEqual(result.mDescription, mDescription);
      } else {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return Arrays.hashCode(new Object[] {mResult, mReader, mWriter, mDescription});
    }
  }

  /**
   * Borrowed from Guava's Objects.equal(a, b)
   */
  private static boolean objectsEqual(Object obj1, Object obj2) {
    return Objects.equals(obj1, obj2);
  }
}
