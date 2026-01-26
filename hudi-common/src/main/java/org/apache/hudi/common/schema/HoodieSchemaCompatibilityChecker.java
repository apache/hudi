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

package org.apache.hudi.common.schema;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Slf4j
public class HoodieSchemaCompatibilityChecker {

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
  public static SchemaPairCompatibility checkReaderWriterCompatibility(final HoodieSchema reader,
                                                                       final HoodieSchema writer,
                                                                       boolean checkNamingOverride) {
    final SchemaCompatibilityResult compatibility =
        new ReaderWriterCompatibilityChecker(checkNamingOverride).getCompatibility(reader, writer);

    final String message;
    switch (compatibility.getCompatibilityType()) {
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
        throw new HoodieException("Unknown compatibility: " + compatibility);
    }

    return new SchemaPairCompatibility(compatibility, reader, writer, message);
  }

  // -----------------------------------------------------------------------------------------------

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
  public static HoodieSchemaField lookupWriterField(final HoodieSchema writerSchema, final HoodieSchemaField readerField) {
    assert (writerSchema.getType() == HoodieSchemaType.RECORD);
    final List<HoodieSchemaField> writerFields = new ArrayList<>();
    writerSchema.getField(readerField.name()).ifPresent(writerFields::add);
    for (final String readerFieldAliasName : readerField.aliases()) {
      writerSchema.getField(readerFieldAliasName).ifPresent(writerFields::add);
    }
    switch (writerFields.size()) {
      case 0:
        return null;
      case 1:
        return writerFields.get(0);
      default: {
        throw new HoodieSchemaException(String.format(
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
  @RequiredArgsConstructor
  private static final class ReaderWriter {
    private final HoodieSchema mReader;
    private final HoodieSchema mWriter;

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
    public SchemaCompatibilityResult getCompatibility(final HoodieSchema reader, final HoodieSchema writer) {
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
    private SchemaCompatibilityResult getCompatibility(final HoodieSchema reader,
                                                       final HoodieSchema writer,
                                                       final Deque<LocationInfo> locations) {
      log.debug("Checking compatibility of reader {} with writer {}", reader, writer);
      final ReaderWriter pair = new ReaderWriter(reader, writer);
      SchemaCompatibilityResult result = mMemoizeMap.get(pair);
      if (result != null) {
        if (result.getCompatibilityType() == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
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

    private static String getLocationName(final Deque<LocationInfo> locations, HoodieSchemaType readerType) {
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
        if (next.type == HoodieSchemaType.MAP) {
          if (locationInfoIterator.hasNext() || readerType != HoodieSchemaType.MAP) {
            sb.append(".value");
          }
        } else if (next.type == HoodieSchemaType.ARRAY) {
          if (locationInfoIterator.hasNext() || readerType != HoodieSchemaType.ARRAY) {
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
     * {@link #getCompatibility(HoodieSchema, HoodieSchema)}.
     * </p>
     *
     * @param reader    Reader schema to test.
     * @param writer    Writer schema to test.
     * @param locations Stack with which to track the location within the schema.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityResult calculateCompatibility(final HoodieSchema reader, final HoodieSchema writer,
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
          case DATE:
          case STRING:
          case UUID:
            return result;
          case TIME:
            return result.mergedWith(checkTimeCompatibility(reader, writer, locations));
          case TIMESTAMP:
            return result.mergedWith(checkTimestampCompatibility(reader, writer, locations));
          case ARRAY:
            return result.mergedWith(getCompatibility(reader.getElementType(), writer.getElementType(), locations));
          case MAP:
            return result.mergedWith(getCompatibility(reader.getValueType(), writer.getValueType(), locations));
          case FIXED:
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkFixedSize(reader, writer, locations));
          case DECIMAL:
            return result.mergedWith(checkDecimalWidening(reader, writer, locations));
          case ENUM:
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkReaderEnumContainsAllWriterEnumSymbols(reader, writer, locations));
          case RECORD:
            result = result.mergedWith(checkSchemaNames(reader, writer, locations));
            return result.mergedWith(checkReaderWriterRecordFields(reader, writer, locations));
          case UNION:
            // Check that each individual branch of the writer union can be decoded:
            for (final HoodieSchema writerBranch : writer.getTypes()) {
              SchemaCompatibilityResult compatibility = getCompatibility(reader, writerBranch, locations);
              if (compatibility.getCompatibilityType() == SchemaCompatibilityType.INCOMPATIBLE) {
                String message = String.format("reader union lacking writer type: %s for field: '%s'", writerBranch.getType(), getLocationName(locations, reader.getType()));
                result = result.mergedWith(SchemaCompatibilityResult.incompatible(
                    SchemaIncompatibilityType.MISSING_UNION_BRANCH, reader, writer, message, asList(locations)));
              }
            }
            // Each schema in the writer union can be decoded with the reader:
            return result;
          default:
            throw new HoodieSchemaException("Unknown schema type: " + reader.getType());
        }

      } else {
        // Reader and writer have different schema types:

        // Reader compatible with all branches of a writer union is compatible
        if (writer.getType() == HoodieSchemaType.UNION) {
          for (HoodieSchema s : writer.getTypes()) {
            result = result.mergedWith(getCompatibility(reader, s, locations));
          }
          return result;
        }

        switch (reader.getType()) {
          case NULL:
          case BOOLEAN:
          case INT:
          case TIME:
          case DATE:
          case TIMESTAMP:
          case DECIMAL:
          case UUID:
            return result.mergedWith(typeMismatch(reader, writer, locations));
          case LONG:
            return (writer.getType() == HoodieSchemaType.INT) ? result : result.mergedWith(typeMismatch(reader, writer, locations));
          case FLOAT:
            return ((writer.getType() == HoodieSchemaType.INT) || (writer.getType() == HoodieSchemaType.LONG)) ? result
                : result.mergedWith(typeMismatch(reader, writer, locations));
          case DOUBLE:
            return ((writer.getType() == HoodieSchemaType.INT) || (writer.getType() == HoodieSchemaType.LONG) || (writer.getType() == HoodieSchemaType.FLOAT))
                ? result
                : result.mergedWith(typeMismatch(reader, writer, locations));
          case BYTES:
            return (writer.getType() == HoodieSchemaType.STRING) ? result : result.mergedWith(typeMismatch(reader, writer, locations));
          case STRING:
            return (writer.getType().isNumeric() || (writer.getType() == HoodieSchemaType.BYTES)
                ? result : result.mergedWith(typeMismatch(reader, writer, locations)));
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
            for (final HoodieSchema readerBranch : reader.getTypes()) {
              SchemaCompatibilityResult compatibility = getCompatibility(readerBranch, writer, locations);
              if (compatibility.getCompatibilityType() == SchemaCompatibilityType.COMPATIBLE) {
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
            throw new HoodieSchemaException("Unknown schema type: " + reader.getType());
          }
        }
      }
    }

    private SchemaCompatibilityResult checkReaderWriterRecordFields(final HoodieSchema reader, final HoodieSchema writer,
                                                                    final Deque<LocationInfo> locations) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();

      // Check that each field in the reader record can be populated from the writer
      // record:
      for (final HoodieSchemaField readerField : reader.getFields()) {
        final HoodieSchemaField writerField = lookupWriterField(writer, readerField);
        if (writerField == null) {
          // Reader field does not correspond to any field in the writer record schema, so
          // the
          // reader field must have a default value.
          if (readerField.defaultVal().isEmpty()) {
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

    private SchemaCompatibilityResult checkReaderEnumContainsAllWriterEnumSymbols(final HoodieSchema reader,
                                                                                  final HoodieSchema writer, final Deque<LocationInfo> locations) {
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

    private SchemaCompatibilityResult checkFixedSize(final HoodieSchema reader, final HoodieSchema writer,
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

    private SchemaCompatibilityResult checkDecimalWidening(final HoodieSchema reader, final HoodieSchema writer,
                                                           final Deque<LocationInfo> locations) {
      boolean isReaderDecimal = reader.getType() == HoodieSchemaType.DECIMAL;
      boolean isWriterDecimal = writer.getType() == HoodieSchemaType.DECIMAL;
      if (!isReaderDecimal && !isWriterDecimal) {
        return SchemaCompatibilityResult.compatible();
      }

      if (!isReaderDecimal || !isWriterDecimal) {
        String message = String.format("Decimal field '%s' expected: %s, found: %s", getLocationName(locations, reader.getType()), writer, reader);
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.DECIMAL_MISMATCH, reader, writer,
            message, asList(locations));
      }

      HoodieSchema.Decimal readerDecimalType = (HoodieSchema.Decimal) reader;
      HoodieSchema.Decimal writerDecimalType = (HoodieSchema.Decimal) writer;
      int readerScale = readerDecimalType.getScale();
      int writerScale = writerDecimalType.getScale();
      int readerPrecision = readerDecimalType.getPrecision();
      int writerPrecision = writerDecimalType.getPrecision();
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

    private SchemaCompatibilityResult checkTimeCompatibility(final HoodieSchema reader, final HoodieSchema writer,
                                                             final Deque<LocationInfo> locations) {
      HoodieSchema.Time readerTime = (HoodieSchema.Time) reader;
      HoodieSchema.Time writerTime = (HoodieSchema.Time) writer;
      if (readerTime.getPrecision() != writerTime.getPrecision()) {
        String message = String.format("Time field '%s' expected precision: %d, found: %d", getLocationName(locations, reader.getType()),
            writerTime.getPrecision(), readerTime.getPrecision());
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH, reader, writer,
            message, asList(locations));
      }
      return SchemaCompatibilityResult.compatible();
    }

    private SchemaCompatibilityResult checkTimestampCompatibility(final HoodieSchema reader, final HoodieSchema writer,
                                                                  final Deque<LocationInfo> locations) {
      HoodieSchema.Timestamp readerTimestamp = (HoodieSchema.Timestamp) reader;
      HoodieSchema.Timestamp writerTimestamp = (HoodieSchema.Timestamp) writer;
      if (readerTimestamp.getPrecision() != writerTimestamp.getPrecision() || readerTimestamp.isUtcAdjusted() != writerTimestamp.isUtcAdjusted()) {
        String message = String.format("Timestamp field '%s' expected precision: %d and utcAdjusted: %b, found: precision: %d and utcAdjusted: %b",
            getLocationName(locations, reader.getType()), writerTimestamp.getPrecision(), writerTimestamp.isUtcAdjusted(),
            readerTimestamp.getPrecision(), readerTimestamp.isUtcAdjusted());
        return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH, reader, writer,
            message, asList(locations));

      }
      return SchemaCompatibilityResult.compatible();
    }

    private SchemaCompatibilityResult checkSchemaNames(final HoodieSchema reader, final HoodieSchema writer,
                                                       final Deque<LocationInfo> locations) {
      checkState(locations.size() > 0);
      // NOTE: We're only going to validate schema names in following cases
      //          - This is a top-level schema (ie enclosing one)
      //          - This is a schema enclosed w/in a union (since in that case schemas could be
      //          reverse-looked up by their fully-qualified names)
      boolean shouldCheckNames = checkNaming && (locations.size() == 1 || locations.peekLast().type == HoodieSchemaType.UNION);
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      if (shouldCheckNames && !Objects.equals(reader.getFullName(), writer.getFullName())) {
        String message = String.format("Reader schema name: '%s' is not compatible with writer schema name: '%s'", reader.getFullName(), writer.getFullName());
        result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.NAME_MISMATCH, reader, writer,
            message, asList(locations));
      }
      return result;
    }

    private SchemaCompatibilityResult typeMismatch(final HoodieSchema reader, final HoodieSchema writer,
                                                   final Deque<LocationInfo> locations) {
      String message = String.format("reader type '%s' not compatible with writer type '%s' for field '%s'", reader.getType(),
          writer.getType(), getLocationName(locations, reader.getType()));
      return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH, reader, writer, message,
          asList(locations));
    }

    @AllArgsConstructor
    @ToString
    public static class LocationInfo {
      private final String name;
      private final HoodieSchemaType type;
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
  @Value
  public static class SchemaCompatibilityResult {

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
      List<Incompatibility> mergedIncompatibilities = new ArrayList<>(incompatibilities);
      mergedIncompatibilities.addAll(toMerge.getIncompatibilities());
      SchemaCompatibilityType mergedCompatibilityType = compatibilityType == SchemaCompatibilityType.COMPATIBLE
          ? toMerge.compatibilityType
          : SchemaCompatibilityType.INCOMPATIBLE;
      return new SchemaCompatibilityResult(mergedCompatibilityType, mergedIncompatibilities);
    }

    SchemaCompatibilityType compatibilityType;
    // the below fields are only valid if INCOMPATIBLE
    List<Incompatibility> incompatibilities;
    // cached objects for stateless details
    private static final SchemaCompatibilityResult COMPATIBLE = new SchemaCompatibilityResult(
        SchemaCompatibilityType.COMPATIBLE, Collections.emptyList());
    private static final SchemaCompatibilityResult RECURSION_IN_PROGRESS = new SchemaCompatibilityResult(
        SchemaCompatibilityType.RECURSION_IN_PROGRESS, Collections.emptyList());

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
                                                         HoodieSchema readerFragment, HoodieSchema writerFragment, String message, List<String> location) {
      Incompatibility incompatibility = new Incompatibility(incompatibilityType, readerFragment, writerFragment,
          message, location);
      return new SchemaCompatibilityResult(SchemaCompatibilityType.INCOMPATIBLE,
          Collections.singletonList(incompatibility));
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Value
  public static class Incompatibility {
    SchemaIncompatibilityType type;
    HoodieSchema readerFragment;
    HoodieSchema writerFragment;
    String message;
    List<String> location;

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
      for (String coordinate : location.subList(1, location.size())) {
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
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Provides information about the compatibility of a single reader and writer
   * schema pair.
   * <p>
   * Note: This class represents a one-way relationship from the reader to the
   * writer schema.
   */
  @AllArgsConstructor
  @Value
  public static class SchemaPairCompatibility {
    /**
     * The details of this result.
     */
    SchemaCompatibilityResult result;

    /**
     * Validated reader schema.
     */
    HoodieSchema reader;

    /**
     * Validated writer schema.
     */
    HoodieSchema writer;

    /**
     * Human-readable description of this result.
     */
    String description;

    /**
     * Gets the type of this result.
     *
     * @return the type of this result.
     */
    public SchemaCompatibilityType getType() {
      return result.getCompatibilityType();
    }
  }
}
