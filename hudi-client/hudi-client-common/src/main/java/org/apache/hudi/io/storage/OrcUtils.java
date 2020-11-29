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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcConf;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OrcUtils {

  public static TypeInfo getOrcField(Schema fieldSchema) throws IllegalArgumentException {
    Schema.Type fieldType = fieldSchema.getType();

    switch (fieldType) {
      case INT:
      case LONG:
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case STRING:
      case NULL:
        return getPrimitiveOrcTypeFromPrimitiveAvroType(fieldType);

      case UNION:
        List<Schema> unionFieldSchemas = fieldSchema.getTypes();

        if (unionFieldSchemas != null) {
          // Ignore null types in union
          List<TypeInfo> orcFields = unionFieldSchemas.stream().filter(
              unionFieldSchema -> !Schema.Type.NULL.equals(unionFieldSchema.getType()))
              .map(OrcUtils::getOrcField)
              .collect(Collectors.toList());

          // Flatten the field if the union only has one non-null element
          if (orcFields.size() == 1) {
            return orcFields.get(0);
          } else {
            return TypeInfoFactory.getUnionTypeInfo(orcFields);
          }
        }
        return null;

      case ARRAY:
        return TypeInfoFactory.getListTypeInfo(getOrcField(fieldSchema.getElementType()));

      case MAP:
        return TypeInfoFactory.getMapTypeInfo(
            getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING),
            getOrcField(fieldSchema.getValueType()));

      case RECORD:
        List<Schema.Field> avroFields = fieldSchema.getFields();
        if (avroFields != null) {
          List<String> orcFieldNames = new ArrayList<>(avroFields.size());
          List<TypeInfo> orcFields = new ArrayList<>(avroFields.size());
          avroFields.forEach(avroField -> {
            String fieldName = avroField.name();
            orcFieldNames.add(fieldName);
            orcFields.add(getOrcField(avroField.schema()));
          });
          return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
        }
        return null;

      case ENUM:
        // An enum value is just a String for ORC/Hive
        return getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING);

      default:
        throw new IllegalArgumentException("Did not recognize Avro type " + fieldType.getName());
    }

  }

  public static TypeInfo getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type avroType) throws IllegalArgumentException {
    if (avroType == null) {
      throw new IllegalArgumentException("Avro type is null");
    }
    switch (avroType) {
      case INT:
        return TypeInfoFactory.getPrimitiveTypeInfo("int");
      case LONG:
        return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
      case BOOLEAN:
      case NULL: // ORC has no null type, so just pick the smallest. All values are necessarily null.
        return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
      case BYTES:
        return TypeInfoFactory.getPrimitiveTypeInfo("binary");
      case DOUBLE:
        return TypeInfoFactory.getPrimitiveTypeInfo("double");
      case FLOAT:
        return TypeInfoFactory.getPrimitiveTypeInfo("float");
      case STRING:
        return TypeInfoFactory.getPrimitiveTypeInfo("string");
      default:
        throw new IllegalArgumentException("Avro type " + avroType.getName() + " is not a primitive type");
    }
  }

  public static org.apache.hadoop.hive.ql.io.orc.Writer createWriter(
      Path path,
      Configuration conf,
      TypeInfo orcSchema,
      long stripeSize,
      CompressionKind compress,
      int bufferSize,
      FileSystem fs) throws IOException {

    int rowIndexStride = (int) OrcConf.ROW_INDEX_STRIDE.getLong(conf);

    boolean addBlockPadding = OrcConf.BLOCK_PADDING.getBoolean(conf);

    String versionName = OrcConf.WRITE_FORMAT.getString(conf);
    OrcFile.Version versionValue = (versionName == null)
        ? OrcFile.Version.CURRENT
        : OrcFile.Version.byName(versionName);

    OrcFile.EncodingStrategy encodingStrategy;
    String enString = OrcConf.ENCODING_STRATEGY.getString(conf);
    if (enString == null) {
      encodingStrategy = OrcFile.EncodingStrategy.SPEED;
    } else {
      encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
    }

    final double paddingTolerance = OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(conf);

    long blockSizeValue = OrcConf.BLOCK_SIZE.getLong(conf);

    double bloomFilterFpp = OrcConf.BLOOM_FILTER_FPP.getDouble(conf);

    ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
        .rowIndexStride(rowIndexStride)
        .blockPadding(addBlockPadding)
        .version(versionValue)
        .encodingStrategy(encodingStrategy)
        .paddingTolerance(paddingTolerance)
        .blockSize(blockSizeValue)
        .bloomFilterFpp(bloomFilterFpp)
        .inspector(inspector)
        .stripeSize(stripeSize)
        .bufferSize(bufferSize)
        .compress(compress)
        .fileSystem(fs);

    return OrcFile.createWriter(path, writerOptions);
  }

  public static Object convertToORCObject(TypeInfo typeInfo, Object o) {
    if (o != null) {
      if (o instanceof Integer) {
        return new IntWritable((int) o);
      }
      if (o instanceof Boolean) {
        return new BooleanWritable((boolean) o);
      }
      if (o instanceof Long) {
        return new LongWritable((long) o);
      }
      if (o instanceof Float) {
        return new FloatWritable((float) o);
      }
      if (o instanceof Double) {
        return new DoubleWritable((double) o);
      }
      if (o instanceof BigDecimal) {
        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) o));
      }
      if (o instanceof String || o instanceof Utf8 || o instanceof GenericData.EnumSymbol) {
        return new Text(o.toString());
      }
      if (o instanceof ByteBuffer) {
        return new BytesWritable(((ByteBuffer) o).array());
      }
      if (o instanceof int[]) {
        int[] intArray = (int[]) o;
        return Arrays.stream(intArray)
            .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("int"), element))
            .collect(Collectors.toList());
      }
      if (o instanceof long[]) {
        long[] longArray = (long[]) o;
        return Arrays.stream(longArray)
            .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), element))
            .collect(Collectors.toList());
      }
      if (o instanceof float[]) {
        float[] floatArray = (float[]) o;
        return IntStream.range(0, floatArray.length)
            .mapToDouble(i -> floatArray[i])
            .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("float"), (float) element))
            .collect(Collectors.toList());
      }
      if (o instanceof double[]) {
        double[] doubleArray = (double[]) o;
        return Arrays.stream(doubleArray)
            .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("double"), element))
            .collect(Collectors.toList());
      }
      if (o instanceof boolean[]) {
        boolean[] booleanArray = (boolean[]) o;
        return IntStream.range(0, booleanArray.length)
            .map(i -> booleanArray[i] ? 1 : 0)
            .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("boolean"), element == 1))
            .collect(Collectors.toList());
      }
      if (o instanceof GenericData.Array) {
        GenericData.Array array = ((GenericData.Array) o);
        // The type information in this case is interpreted as a List
        TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
        return array.stream().map((element) -> convertToORCObject(listTypeInfo, element)).collect(Collectors.toList());
      }
      if (o instanceof List) {
        return o;
      }
      if (o instanceof Map) {
        Map map = new HashMap();
        TypeInfo keyInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
        TypeInfo valueInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
        // Unions are not allowed as key/value types, so if we convert the key and value objects,
        // they should return Writable objects
        ((Map) o).forEach((key, value) -> {
          Object keyObject = convertToORCObject(keyInfo, key);
          Object valueObject = convertToORCObject(valueInfo, value);
          if (keyObject == null) {
            throw new IllegalArgumentException("Maps' key cannot be null");
          }
          map.put(keyObject, valueObject);
        });
        return map;
      }
      if (o instanceof GenericData.Record) {
        GenericData.Record record = (GenericData.Record) o;
        TypeInfo recordSchema = OrcUtils.getOrcField(record.getSchema());
        List<Schema.Field> recordFields = record.getSchema().getFields();
        if (recordFields != null) {
          Object[] fieldObjects = new Object[recordFields.size()];
          for (int i = 0; i < recordFields.size(); i++) {
            Schema.Field field = recordFields.get(i);
            Schema fieldSchema = field.schema();
            Object fieldObject = record.get(field.name());
            fieldObjects[i] = OrcUtils.convertToORCObject(OrcUtils.getOrcField(fieldSchema), fieldObject);
          }
          return OrcUtils.createOrcStruct(recordSchema, fieldObjects);
        }
      }
      throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo.getTypeName());
    } else {
      return null;
    }
  }

  public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
    SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct
        .createObjectInspector(typeInfo);
    List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
    OrcStruct result = (OrcStruct) oi.create();
    result.setNumFields(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      oi.setStructFieldData(result, fields.get(i), objs[i]);
    }
    return result;
  }

  public static Configuration registerFileSystem(Path file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

}
