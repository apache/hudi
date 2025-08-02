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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utils to generate schemas for schema on read evolution tests.
 * Also to generate column name changes.
 */
public class SchemaOnReadEvolutionTestUtils extends SchemaEvolutionTestUtilsBase {

  /**
   * generate the schema for schema on read evolution testing
   *
   * @param configs        test configs to decide which evolution cases to run
   * @param iteration      the current iteration of evolution
   * @param maxIterations  the maximum number of iterations that this test should run.
   *                       it is 0 indexed so if you evolve 3 times, maxIterations should be 2
   *
   * @return the generated schema
   */
  public static InternalSchema generateExtendedSchema(SchemaOnReadConfigs configs, int iteration, int maxIterations) {
    validateConfigs(configs);

    List<SchemaEvolutionField> baseFields = buildBaseFieldsList(configs, iteration, maxIterations);
    Schema tmpExtendedSchema = generateExtendedSchema(configs, baseFields);
    InternalSchema modificationSchema = AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(tmpExtendedSchema));

    SchemaModifier modifier = new SchemaModifier(configs);

    // Apply initial schema modifications
    modificationSchema = addInitialFields(modificationSchema, configs, maxIterations, modifier);

    // Apply iterative modifications
    modificationSchema = applyUpdates(modificationSchema, configs, modifier, iteration, maxIterations);

    return modificationSchema;
  }
  
  /**
   * Generate map from old column name to new column name for a single iteration
   *
   * @param configs          test configs to decide which evolution cases to run
   * @param iteration        the current iteration of evolution
   * @param maxIterations    the maximum number of iterations that this test should run.
   *                         it is 0 indexed so if you evolve 3 times, maxIterations should be 2
   *
   * @return map from old column name to new column name
   */
  public static Map<String, String> generateColumnNameChanges(SchemaOnReadConfigs configs, int iteration, int maxIterations) {
    if (!configs.renameColumnSupport || iteration == 0) {
      return Collections.emptyMap();
    }

    RenameCapture modifier = new RenameCapture(configs);
    applyUpdateIteration(InternalSchema.getEmptyInternalSchema(), configs, modifier, iteration, maxIterations);
    return modifier.getRenameMap();
  }

  public static class SchemaOnReadConfigs extends SchemaEvolutionTestUtilsBase.SchemaEvolutionConfigBase {
    public boolean addNewFieldSupport = true;
    public boolean addNewFieldNotAtEndSupport = true;
    public boolean reorderColumnSupport = true;
    public boolean renameColumnSupport = true;
    public boolean removeColumnSupport = true;
    public boolean renameColumnAsPreviouslyRemovedSupport = true;

    // Int
    public boolean intToDecimalFixedSupport = true;
    public boolean intToDecimalBytesSupport = true;

    // Long
    public boolean longToDecimalFixedSupport = true;
    public boolean longToDecimalBytesSupport = true;

    // Float
    public boolean floatToDecimalFixedSupport = true;
    public boolean floatToDecimalBytesSupport = true;

    // Double
    public boolean doubleToDecimalFixedSupport = true;
    public boolean doubleToDecimalBytesSupport = true;

    // String
    public boolean stringToDecimalFixedSupport = true;
    public boolean stringToDecimalBytesSupport = true;
    public boolean stringToDateSupport = true;

    // Decimal
    public boolean decimalFixedToStringSupport = true;
    public boolean decimalBytesToStringSupport = true;

    // Date
    public boolean dateToStringSupport = true;
  }

  //TODO: decide if logical types should have more cases
  // also if we need to test evolution where the main type is the same but the logical type changes
  private enum SchemaOnReadTypePromotionCase {
    INT_TO_INT(Schema.Type.INT, null, Schema.Type.INT, null, config -> true),
    INT_TO_LONG(Schema.Type.INT, null, Schema.Type.LONG, null, config -> config.intToLongSupport),
    INT_TO_FLOAT(Schema.Type.INT, null, Schema.Type.FLOAT, null, config -> config.intToFloatSupport),
    INT_TO_DOUBLE(Schema.Type.INT, null, Schema.Type.DOUBLE, null, config -> config.intToDoubleSupport),
    INT_TO_STRING(Schema.Type.INT, null, Schema.Type.STRING, null, config -> config.intToStringSupport),
    INT_TO_DECIMAL_FIXED(Schema.Type.INT, null, Schema.Type.FIXED, LogicalTypes.decimal(10, 3), config -> config.intToDecimalFixedSupport),
    INT_TO_DECIMAL_BYTES(Schema.Type.INT, null, Schema.Type.BYTES, LogicalTypes.decimal(10, 3), config -> config.intToDecimalBytesSupport),

    LONG_TO_LONG(Schema.Type.LONG, null, Schema.Type.LONG, null, config -> true),
    LONG_TO_FLOAT(Schema.Type.LONG, null, Schema.Type.FLOAT, null, config -> config.longToFloatSupport),
    LONG_TO_DOUBLE(Schema.Type.LONG, null, Schema.Type.DOUBLE, null, config -> config.longToDoubleSupport),
    LONG_TO_STRING(Schema.Type.LONG, null, Schema.Type.STRING, null, config -> config.longToStringSupport),
    LONG_TO_DECIMAL_FIXED(Schema.Type.LONG, null, Schema.Type.FIXED, LogicalTypes.decimal(10, 3), config -> config.longToDecimalFixedSupport),
    LONG_TO_DECIMAL_BYTES(Schema.Type.LONG, null, Schema.Type.BYTES, LogicalTypes.decimal(10, 3), config -> config.longToDecimalBytesSupport),

    FLOAT_TO_FLOAT(Schema.Type.FLOAT, null, Schema.Type.FLOAT, null, config -> true),
    FLOAT_TO_DOUBLE(Schema.Type.FLOAT, null, Schema.Type.DOUBLE, null, config -> config.floatToDoubleSupport),
    FLOAT_TO_STRING(Schema.Type.FLOAT, null, Schema.Type.STRING, null, config -> config.floatToStringSupport),
    FLOAT_TO_DECIMAL_FIXED(Schema.Type.FLOAT, null, Schema.Type.FIXED, LogicalTypes.decimal(10, 3), config -> config.floatToDecimalFixedSupport),
    FLOAT_TO_DECIMAL_BYTES(Schema.Type.FLOAT, null, Schema.Type.BYTES, LogicalTypes.decimal(10, 3), config -> config.floatToDecimalBytesSupport),

    DOUBLE_TO_DOUBLE(Schema.Type.DOUBLE, null, Schema.Type.DOUBLE, null, config -> true),
    DOUBLE_TO_STRING(Schema.Type.DOUBLE, null, Schema.Type.STRING, null, config -> config.doubleToStringSupport),
    DOUBLE_TO_DECIMAL_FIXED(Schema.Type.DOUBLE, null, Schema.Type.FIXED, LogicalTypes.decimal(10, 3), config -> config.doubleToDecimalFixedSupport),
    DOUBLE_TO_DECIMAL_BYTES(Schema.Type.DOUBLE, null, Schema.Type.BYTES, LogicalTypes.decimal(10, 3), config -> config.doubleToDecimalBytesSupport),

    STRING_TO_STRING(Schema.Type.STRING, null, Schema.Type.STRING, null, config -> true),
    STRING_TO_DECIMAL_FIXED(Schema.Type.STRING, null, Schema.Type.FIXED, LogicalTypes.decimal(10, 3), config -> config.stringToDecimalFixedSupport),
    STRING_TO_DECIMAL_BYTES(Schema.Type.STRING, null, Schema.Type.BYTES, LogicalTypes.decimal(10, 3), config -> config.stringToDecimalBytesSupport),
    STRING_TO_DATE(Schema.Type.STRING, null, Schema.Type.INT, LogicalTypes.date(), config -> config.stringToDateSupport),

    DECIMAL_FIXED_TO_STRING(Schema.Type.FIXED, LogicalTypes.decimal(4, 3), Schema.Type.STRING, null, config -> config.decimalFixedToStringSupport),
    DECIMAL_BYTES_TO_STRING(Schema.Type.BYTES, LogicalTypes.decimal(4, 3), Schema.Type.STRING, null, config -> config.decimalBytesToStringSupport),
    DATE_TO_STRING(Schema.Type.INT, LogicalTypes.date(), Schema.Type.STRING, null, config -> config.dateToStringSupport);

    public final Schema.Type before;
    public final Schema.Type after;
    public final LogicalType logicalTypeBefore;
    public final LogicalType logicalTypeAfter;
    public final Predicate<SchemaOnReadConfigs> isEnabled;

    public String getFormattedName() {
      return Arrays.stream(this.name().split("_"))
          .map(word -> word.charAt(0) + word.substring(1).toLowerCase())
          .collect(Collectors.joining(""));
    }

    SchemaOnReadTypePromotionCase(Schema.Type before, LogicalType logicalTypeBefore,
                                  Schema.Type after, LogicalType logicalTypeAfter,
                                  Predicate<SchemaOnReadConfigs> isEnabled) {
      this.before = before;
      this.logicalTypeBefore = logicalTypeBefore;
      this.after = after;
      this.logicalTypeAfter = logicalTypeAfter;
      this.isEnabled = isEnabled;
    }
  }

  // Evolutions that add/remove/rename/reorder fields as opposed to changing their type
  private enum SchemaOnReadStructuralCase {

    REORDER_COLUMNS(c -> c.reorderColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialReorderFields,
        SchemaOnReadEvolutionTestUtils::applyReorderModifications
    ),

    ADD_FIELD_NOT_AT_END(c -> c.addNewFieldNotAtEndSupport,
        SchemaOnReadEvolutionTestUtils::addInitialNonEndFields,
        SchemaOnReadEvolutionTestUtils::applyNonEndFieldModifications
    ),

    RENAME_COLUMN(c -> c.renameColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRenameFields,
        SchemaOnReadEvolutionTestUtils::applyRenameModifications
    ),

    REMOVE_COLUMN(c -> c.removeColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRemoveFields,
        SchemaOnReadEvolutionTestUtils::applyRemoveModifications
    ),

    RENAME_AS_REMOVED(c -> c.renameColumnAsPreviouslyRemovedSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRenameAsRemovedFields,
        SchemaOnReadEvolutionTestUtils::applyRenameAsRemovedModifications
    ),

    ADD_FIELD_AT_END(c -> c.addNewFieldSupport,
        SchemaOnReadEvolutionTestUtils::addInitialEndFields,
        SchemaOnReadEvolutionTestUtils::applyEndFieldModifications
    );

    private final Function<SchemaOnReadConfigs, Boolean> isEnabled;
    private final TriFunction<InternalSchema, Integer, SchemaModifier, InternalSchema> initializationFunction;
    private final QuadFunction<InternalSchema, SchemaModifier, Integer, Integer, InternalSchema> updateFunction;

    SchemaOnReadStructuralCase(
        Function<SchemaOnReadConfigs, Boolean> isEnabled,
        TriFunction<InternalSchema, Integer, SchemaModifier, InternalSchema> initializationFunction,
        QuadFunction<InternalSchema, SchemaModifier, Integer, Integer, InternalSchema> updateFunction) {
      this.isEnabled = isEnabled;
      this.initializationFunction = initializationFunction;
      this.updateFunction = updateFunction;
    }

    public boolean enabled(SchemaOnReadConfigs configs) {
      return isEnabled.apply(configs);
    }

    // need to add fields that we can apply structural changes to
    public InternalSchema applyInitialization(InternalSchema schema, int maxIterations, SchemaModifier modifier) {
      return initializationFunction.apply(schema, maxIterations, modifier);
    }

    // subsequent evolutions will modify the structural columns
    public InternalSchema applyUpdate(InternalSchema schema, SchemaModifier modifier, int iteration, int maxIterations) {
      return updateFunction.apply(schema, modifier, iteration, maxIterations);
    }

    @FunctionalInterface
    public interface QuadFunction<A, B, C, D, R> {
      R apply(A a, B b, C c, D d);
    }

    @FunctionalInterface
    public interface TriFunction<A, B, C, R> {
      R apply(A a, B b, C c);
    }
  }

  private static void validateConfigs(SchemaOnReadConfigs configs) {
    if (configs.renameColumnAsPreviouslyRemovedSupport) {
      if (!configs.renameColumnSupport || !configs.removeColumnSupport) {
        throw new IllegalArgumentException(
            "renameColumnAsPreviouslyRemovedSupport requires renameColumnSupport and removeColumnSupport to be enabled");
      }
    }

    if (configs.arraySupport) {
      if (!configs.anyArraySupport) {
        throw new IllegalArgumentException("arraySupport requires anyArraySupport to be enabled");
      }
    }
  }

  // base fields are used for type promotion. If we are doing n iterations, we want to have n sets of base fields so that we can
  // promote one set on each iteration
  private static List<SchemaEvolutionField> buildBaseFieldsList(SchemaOnReadConfigs configs, int iteration, int maxIterations) {
    List<SchemaEvolutionField> baseFields = new ArrayList<>();
    for (int i = 0; i < maxIterations; i++) {
      for (SchemaOnReadTypePromotionCase evolution : SchemaOnReadTypePromotionCase.values()) {
        if (evolution.isEnabled.test(configs)) {
          if (i >= iteration) {
            baseFields.add(new SchemaEvolutionField(evolution.getFormattedName(), evolution.before, evolution.logicalTypeBefore));
          } else {
            baseFields.add(new SchemaEvolutionField(evolution.getFormattedName(), evolution.after, evolution.logicalTypeAfter));
          }
        }
      }
    }
    return baseFields;
  }

  // add fields that we can apply structural changes to
  private static InternalSchema addInitialFields(InternalSchema schema, SchemaOnReadConfigs configs, Integer maxIterations, SchemaModifier modifier) {
    InternalSchema result = schema;
    for (SchemaOnReadStructuralCase strategy : SchemaOnReadStructuralCase.values()) {
      if (strategy.enabled(configs)) {
        result = strategy.applyInitialization(result, maxIterations, modifier);
      }
    }
    return result;
  }

  // initialize fields for REORDER_COLUMNS
  private static InternalSchema addInitialReorderFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    List<FieldNameHolder> fieldNamesList = new ArrayList<>();
    for (int i = 0; i <= maxIterations; i++) {
      fieldNamesList.add(FieldNames.reorderField(i));
    }
    return modifier.addFields(schema, fieldNamesList);
  }

  // initialize fields for ADD_FIELD_NOT_AT_END
  private static InternalSchema addInitialNonEndFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    return modifier.addFields(schema, Collections.singletonList(FieldNames.addField(0)));
  }

  // initialize fields for RENAME_COLUMN
  private static InternalSchema addInitialRenameFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    List<FieldNameHolder> renameFields = new ArrayList<>(maxIterations);
    for (int i = 0; i < maxIterations; i++) {
      renameFields.add(FieldNames.renameField(i, 0));
    }
    return modifier.addFields(schema, renameFields);
  }

  // initialize fields for REMOVE_COLUMN
  private static InternalSchema addInitialRemoveFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    List<FieldNameHolder> removeFields = new ArrayList<>(maxIterations);
    for (int i = 0; i < maxIterations; i++) {
      removeFields.add(FieldNames.removeField(i));
    }
    return modifier.addFields(schema, removeFields);
  }

  // initialize fields for RENAME_AS_REMOVED
  private static InternalSchema addInitialRenameAsRemovedFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    List<FieldNameHolder> renameAsRemovedFields = new ArrayList<>(maxIterations);
    for (int i = 0; i < maxIterations; i++) {
      renameAsRemovedFields.add(FieldNames.removedForRenameField(i));
      renameAsRemovedFields.add(FieldNames.renameToRemovedField(i));
    }
    return modifier.addFields(schema, renameAsRemovedFields);
  }

  // initialize fields for ADD_FIELD_AT_END
  private static InternalSchema addInitialEndFields(InternalSchema schema, Integer maxIterations, SchemaModifier modifier) {
    return modifier.addFields(schema, Collections.singletonList(FieldNames.addEndField(0)));
  }

  // evolve the structural columns. We start with the initial structural columns and need to apply
  // all the previous structural evolutions
  private static InternalSchema applyUpdates(InternalSchema schema, SchemaOnReadConfigs configs,
                                             SchemaModifier modifier, int iteration, int maxIterations) {
    InternalSchema result = schema;
    for (int i = 1; i <= iteration; i++) {
      result = applyUpdateIteration(result, configs, modifier, i, maxIterations);
    }
    return result;
  }

  // single round of evolution for structural columns
  private static InternalSchema applyUpdateIteration(InternalSchema schema, SchemaOnReadConfigs configs,
                                                     SchemaModifier modifier, int iteration, int maxIterations) {
    InternalSchema result = schema;
    for (SchemaOnReadStructuralCase strategy : SchemaOnReadStructuralCase.values()) {
      if (strategy.enabled(configs)) {
        result = strategy.applyUpdate(result, modifier, iteration, maxIterations);
      }
    }
    return result;
  }

  // evolve the REORDER_COLUMNS fields
  private static InternalSchema applyReorderModifications(InternalSchema schema, SchemaModifier modifier,
                                                          int iteration, int maxIterations) {
    // rotate the fields. So 0, 1, 2, 3, 4 becomes 4, 0, 1, 2, 3 becomes 3, 4, 0, 1, 2 etc
    int sourceIndex = maxIterations + 1 - iteration;
    int targetIndex = (maxIterations + 2 - iteration) % (maxIterations + 1);

    FieldNameHolder sourceFields = FieldNames.reorderField(sourceIndex);
    FieldNameHolder targetFields = FieldNames.reorderField(targetIndex);

    return modifier.repositionFields(schema, sourceFields, targetFields,
        TableChange.ColumnPositionChange.ColumnPositionType.BEFORE);
  }

  // evolve the ADD_FIELD_NOT_AT_END fields
  private static InternalSchema applyNonEndFieldModifications(InternalSchema schema, SchemaModifier modifier,
                                                              int iteration, int maxIterations) {
    // Add new fields
    InternalSchema tempSchema = modifier.addFields(schema, Collections.singletonList(FieldNames.addField(iteration)));
    // Position the new fields not at the end
    FieldNameHolder currentFields = FieldNames.addField(iteration);
    FieldNameHolder previousFields = FieldNames.addField(iteration - 1);
    return modifier.repositionFields(tempSchema,
        currentFields, previousFields,
        TableChange.ColumnPositionChange.ColumnPositionType.AFTER);
  }

  // evolve the RENAME_COLUMN fields
  private static InternalSchema applyRenameModifications(InternalSchema schema, SchemaModifier modifier,
                                                         int iteration, int maxIterations) {
    InternalSchema result = schema;

    for (int j = 0; j < iteration; j++) {
      int oldRevision = iteration - 1 - j;
      int newRevision = iteration - j;
      FieldNameHolder oldFields = FieldNames.renameField(j, oldRevision);
      FieldNameHolder newFields = FieldNames.renameField(j, newRevision);
      result = modifier.renameFields(result, oldFields, newFields);
    }

    return result;
  }

  // evolve the REMOVE_COLUMN fields
  private static InternalSchema applyRemoveModifications(InternalSchema schema, SchemaModifier modifier,
                                                         int iteration, int maxIterations) {
    int indexToRemove = iteration - 1;
    FieldNameHolder fieldsToRemove = FieldNames.removeField(indexToRemove);
    return modifier.deleteFields(schema, fieldsToRemove);
  }

  // evolve the RENAME_AS_REMOVED fields
  // we delete a field and then rename another field to the name of the deleted field
  private static InternalSchema applyRenameAsRemovedModifications(InternalSchema schema, SchemaModifier modifier,
                                                                  int iteration, int maxIterations) {
    int index = iteration - 1;

    // First delete the fields
    FieldNameHolder fieldsToDelete = FieldNames.removedForRenameField(index);
    InternalSchema tempSchema = modifier.deleteFields(schema, fieldsToDelete);

    // Then rename the other fields
    FieldNameHolder oldFields = FieldNames.renameToRemovedField(index);
    FieldNameHolder newFields = FieldNames.removedForRenameField(index);

    return modifier.renameFields(tempSchema, oldFields, newFields);
  }

  // evolve the ADD_FIELD_AT_END fields
  private static InternalSchema applyEndFieldModifications(InternalSchema schema, SchemaModifier modifier,
                                                           int iteration, int maxIterations) {
    FieldNameHolder endFields = FieldNames.addEndField(iteration);
    return modifier.addFields(schema, Collections.singletonList(endFields));
  }

  // We do the same operations for base, nested, array, and map fields
  // hold them all together and then we can apply operations on them all at once
  private static class FieldNameHolder {
    public final String baseFieldName;
    public final String nestedFieldName;
    public final String arrayFieldName;
    public final String mapFieldName;

    public FieldNameHolder(String baseFieldName, String nestedFieldName, String arrayFieldName, String mapFieldName) {
      this.baseFieldName = baseFieldName;
      this.nestedFieldName = nestedFieldName;
      this.arrayFieldName = arrayFieldName;
      this.mapFieldName = mapFieldName;
    }
  }

  // Field name generators for test cases
  private static class FieldNames {

    // Helper method to create FieldNameHolder with consistent naming pattern
    private static FieldNameHolder createFieldNameHolder(String operation, String suffix) {
      return new FieldNameHolder(
          "customField" + operation + suffix,
          "customFare" + operation + suffix,
          "customFieldArray" + operation + suffix,
          "customFieldMap" + operation + suffix
      );
    }

    // for SchemaOnReadStructuralCase.REORDER_COLUMNS
    static FieldNameHolder reorderField(int i) {
      return createFieldNameHolder("Reorder", String.valueOf(i));
    }

    // for SchemaOnReadStructuralCase.ADD_FIELD_NOT_AT_END
    static FieldNameHolder addField(int i) {
      return createFieldNameHolder("Add", String.valueOf(i));
    }

    // for SchemaOnReadStructuralCase.RENAME_COLUMN
    static FieldNameHolder renameField(int i, int revision) {
      return createFieldNameHolder("Rename", i + "r" + revision);
    }

    // for SchemaOnReadStructuralCase.REMOVE_COLUMN
    static FieldNameHolder removeField(int i) {
      return createFieldNameHolder("Remove", String.valueOf(i));
    }

    // for SchemaOnReadStructuralCase.RENAME_AS_REMOVED
    static FieldNameHolder removedForRenameField(int i) {
      return createFieldNameHolder("RemovedForRename", String.valueOf(i));
    }

    // also for SchemaOnReadStructuralCase.RENAME_AS_REMOVED
    static FieldNameHolder renameToRemovedField(int i) {
      return createFieldNameHolder("RenameToRemoved", String.valueOf(i));
    }

    // for SchemaOnReadStructuralCase.ADD_FIELD_AT_END
    static FieldNameHolder addEndField(int i) {
      return createFieldNameHolder("AddEnd", String.valueOf(i));
    }
  }

  // Apply base, nested, array and map modifications at once
  private static class SchemaModifier {
    protected final SchemaOnReadConfigs configs;

    // Structure definitions as enum
    protected enum StructureType {
      BASE("", null, holder -> holder.baseFieldName),
      NESTED("fare.", "fare", holder -> holder.nestedFieldName),
      ARRAY("customFieldArray.element.", "customFieldArray.element", holder -> holder.arrayFieldName),
      MAP("customFieldMap.value.", "customFieldMap.value", holder -> holder.mapFieldName);

      final String prefix;
      final String parent; // for add operations
      final Function<FieldNameHolder, String> fieldExtractor;

      StructureType(String prefix, String parent,
                    Function<FieldNameHolder, String> fieldExtractor) {
        this.prefix = prefix;
        this.parent = parent;
        this.fieldExtractor = fieldExtractor;
      }

      boolean isEnabled(SchemaOnReadConfigs configs) {
        switch (this) {
          case BASE: return true;
          case NESTED: return configs.nestedSupport;
          case ARRAY: return configs.arraySupport;
          case MAP: return configs.mapSupport;
          default: return false;
        }
      }
    }

    private SchemaModifier(SchemaOnReadConfigs configs) {
      this.configs = configs;
    }

    InternalSchema addFields(InternalSchema schema, List<FieldNameHolder> fieldNamesList) {
      TableChanges.ColumnAddChange columnAddChange = TableChanges.ColumnAddChange.get(schema);
      for (FieldNameHolder fieldNames : fieldNamesList) {
        for (StructureType structure : StructureType.values()) {
          if (structure.isEnabled(configs)) {
            String fieldName = structure.fieldExtractor.apply(fieldNames);
            if (structure.parent == null) {
              columnAddChange.addColumns(fieldName, Types.StringType.get(), "");
            } else {
              columnAddChange.addColumns(structure.parent, fieldName, Types.StringType.get(), "");
            }
          }
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnAddChange);
    }

    InternalSchema deleteFields(InternalSchema schema, FieldNameHolder fieldNames) {
      TableChanges.ColumnDeleteChange columnDeleteChange = TableChanges.ColumnDeleteChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String fieldName = structure.fieldExtractor.apply(fieldNames);
          columnDeleteChange.deleteColumn(structure.prefix + fieldName);
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnDeleteChange);
    }

    InternalSchema renameFields(InternalSchema schema,
                                FieldNameHolder oldFieldNames,
                                FieldNameHolder newFieldNames) {
      TableChanges.ColumnUpdateChange columnUpdateChange = TableChanges.ColumnUpdateChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String oldFieldName = structure.fieldExtractor.apply(oldFieldNames);
          String newFieldName = structure.fieldExtractor.apply(newFieldNames);
          columnUpdateChange.renameColumn(structure.prefix + oldFieldName, newFieldName);
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnUpdateChange);
    }

    InternalSchema repositionFields(InternalSchema schema,
                                    FieldNameHolder fieldNames,
                                    FieldNameHolder refFieldNames,
                                    TableChange.ColumnPositionChange.ColumnPositionType positionType) {
      TableChanges.ColumnUpdateChange columnUpdateChange = TableChanges.ColumnUpdateChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String fieldName = structure.fieldExtractor.apply(fieldNames);
          String refFieldName = structure.fieldExtractor.apply(refFieldNames);
          columnUpdateChange.addPositionChange(
              structure.prefix + fieldName,
              structure.prefix + refFieldName,
              positionType);
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnUpdateChange);
    }
  }

  // For validation of the evolution, we need a map from new field name to old field name.
  // Instead of modifying the schema, we capture the rename operations from the SchemaOnReadStructuralCase
  // updates and store them in this map
  private static class RenameCapture extends SchemaModifier {

    private final Map<String, String> renameMap = new HashMap<>();

    private RenameCapture(SchemaOnReadConfigs configs) {
      super(configs);
    }

    @Override
    InternalSchema addFields(InternalSchema schema, List<FieldNameHolder> fieldNamesList) {
      return schema;
    }

    @Override
    InternalSchema deleteFields(InternalSchema schema, FieldNameHolder fieldNames) {
      return schema;
    }

    @Override
    InternalSchema renameFields(InternalSchema schema,
                                FieldNameHolder oldFieldNames,
                                FieldNameHolder newFieldNames) {
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String oldFieldName = structure.fieldExtractor.apply(oldFieldNames);
          String newFieldName = structure.fieldExtractor.apply(newFieldNames);
          renameMap.put(structure.prefix + newFieldName, oldFieldName);
        }
      }
      return schema;
    }

    @Override
    InternalSchema repositionFields(InternalSchema schema,
                                            FieldNameHolder fieldNames,
                                            FieldNameHolder refFieldNames,
                                            TableChange.ColumnPositionChange.ColumnPositionType positionType) {
      return schema;
    }

    public Map<String, String> getRenameMap() {
      return renameMap;
    }
  }
}
