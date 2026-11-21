/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.util;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

/**
 * Converts blocks from the type the Trino parquet reader can decode a column as to the column's Trino type, for the two
 * {@link TrinoTypeMapper} types the reader cannot produce from the layouts Hudi writes:
 * <ul>
 * <li>UUID: Hudi writes it as a string by default and as a 16-byte FIXED_LEN_BYTE_ARRAY with the UUID annotation only
 * under {@code parquet.avro.write-parquet-uuid=true}, while the reader decodes {@code uuid} from the latter only. Both
 * are read as varbinary and converted.</li>
 * <li>VECTOR: written as a bare FIXED_LEN_BYTE_ARRAY of little-endian elements, which the reader cannot decode into an
 * array. Read as varbinary and decoded into an array of the element type.</li>
 * </ul>
 * Both are detected from the column type and the file's parquet type alone, so workers need no table schema: a
 * {@code uuid} column is a UUID, and an array of real, double or tinyint over a non-repeated FIXED_LEN_BYTE_ARRAY is a
 * VECTOR. Rows, arrays and maps are walked alongside their parquet groups so nested UUID and VECTOR values convert too;
 * subtrees that need no conversion pass through as they are.
 */
public final class ParquetReadConversions
{
    // Builds the map read types below, whose operators the parquet reader never invokes
    private static final TypeOperators READ_TYPE_OPERATORS = new TypeOperators();

    private ParquetReadConversions() {}

    /**
     * @param readType the type the parquet reader decodes the column as
     * @param toColumnType converts a block of {@code readType} into a block of the column type
     */
    public record ReadConversion(Type readType, Function<Block, Block> toColumnType)
    {
        public ReadConversion
        {
            requireNonNull(readType, "readType is null");
            requireNonNull(toColumnType, "toColumnType is null");
        }
    }

    /**
     * Returns the conversion a column of {@code columnType} stored as {@code parquetType} needs, or empty when the
     * reader decodes it as the column type directly.
     */
    public static Optional<ReadConversion> forColumn(Type columnType, org.apache.parquet.schema.Type parquetType)
    {
        if (columnType instanceof UuidType) {
            if (!isBinary(parquetType)) {
                return Optional.empty();
            }
            return Optional.of(new ReadConversion(VARBINARY, ParquetReadConversions::uuidsFromBinary));
        }
        if (columnType instanceof ArrayType arrayType) {
            if (isVectorLayout(arrayType, parquetType)) {
                return Optional.of(new ReadConversion(VARBINARY, block -> vectorsFromBinary(arrayType, block)));
            }
            return arrayElementType(parquetType)
                    .flatMap(elementParquetType -> forColumn(arrayType.getElementType(), elementParquetType))
                    .map(element -> new ReadConversion(
                            new ArrayType(element.readType()),
                            block -> convertArrayElements(block, element.toColumnType())));
        }
        if (columnType instanceof MapType mapType) {
            // Map keys are always varchar (TrinoTypeMapper maps Avro's string keys), so only values can need a conversion
            return mapValueType(parquetType)
                    .flatMap(valueParquetType -> forColumn(mapType.getValueType(), valueParquetType))
                    .map(value -> new ReadConversion(
                            new MapType(mapType.getKeyType(), value.readType(), READ_TYPE_OPERATORS),
                            block -> convertMapValues(block, mapType, value.toColumnType())));
        }
        if (columnType instanceof RowType rowType) {
            if (parquetType.isPrimitive()) {
                return Optional.empty();
            }
            return rowConversion(rowType, parquetType.asGroupType());
        }
        return Optional.empty();
    }

    private static Optional<ReadConversion> rowConversion(RowType rowType, GroupType groupType)
    {
        List<RowType.Field> fields = rowType.getFields();
        List<RowType.Field> readFields = new ArrayList<>(fields.size());
        List<Optional<Function<Block, Block>>> fieldConversions = new ArrayList<>(fields.size());
        boolean anyConversion = false;
        for (RowType.Field field : fields) {
            // Resolved by name, as the parquet reader resolves row fields
            Optional<ReadConversion> conversion = field.getName()
                    .map(name -> getParquetTypeByName(name, groupType))
                    .flatMap(fieldParquetType -> forColumn(field.getType(), fieldParquetType));
            anyConversion |= conversion.isPresent();
            readFields.add(new RowType.Field(field.getName(), conversion.map(ReadConversion::readType).orElse(field.getType())));
            fieldConversions.add(conversion.map(ReadConversion::toColumnType));
        }
        if (!anyConversion) {
            return Optional.empty();
        }
        return Optional.of(new ReadConversion(RowType.from(readFields), block -> convertRowFields(block, fieldConversions)));
    }

    private static boolean isBinary(org.apache.parquet.schema.Type parquetType)
    {
        if (!parquetType.isPrimitive()) {
            return false;
        }
        PrimitiveTypeName primitiveType = parquetType.asPrimitiveType().getPrimitiveTypeName();
        return primitiveType == PrimitiveTypeName.BINARY || primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    }

    /**
     * A VECTOR is an array of real, double or tinyint whose file column is a single FIXED_LEN_BYTE_ARRAY rather than a
     * repeated field, which no parquet list encoding can be.
     */
    private static boolean isVectorLayout(ArrayType arrayType, org.apache.parquet.schema.Type parquetType)
    {
        Type elementType = arrayType.getElementType();
        return (REAL.equals(elementType) || DOUBLE.equals(elementType) || TINYINT.equals(elementType))
                && parquetType.isPrimitive()
                && !parquetType.isRepetition(REPEATED)
                && parquetType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    }

    /**
     * The element type of a parquet list, following the backward-compatibility rules
     * {@code ParquetTypeUtils.getArrayElementColumn} applies to the same schema.
     */
    private static Optional<org.apache.parquet.schema.Type> arrayElementType(org.apache.parquet.schema.Type parquetType)
    {
        if (parquetType.isPrimitive()) {
            // A bare repeated primitive is a required list of required elements of that type
            return parquetType.isRepetition(REPEATED) ? Optional.of(parquetType) : Optional.empty();
        }
        org.apache.parquet.schema.Type parent = parquetType;
        org.apache.parquet.schema.Type repeated = parquetType;
        while (!repeated.isPrimitive() && !repeated.isRepetition(REPEATED)) {
            GroupType group = repeated.asGroupType();
            if (group.getFieldCount() != 1) {
                return Optional.empty();
            }
            parent = repeated;
            repeated = group.getType(0);
        }
        // Standard three-level list: the repeated group wraps the element
        if (!repeated.isPrimitive()
                && repeated.getLogicalTypeAnnotation() == null
                && repeated.asGroupType().getFieldCount() == 1
                && !repeated.getName().equals("array")
                && !repeated.getName().equals(parent.getName() + "_tuple")) {
            return Optional.of(repeated.asGroupType().getType(0));
        }
        // Two-level list: the repeated field is the element
        return Optional.of(repeated);
    }

    /**
     * The value type of a parquet map, found the way {@code ParquetTypeUtils.getMapKeyValueColumn} finds the key-value
     * group.
     */
    private static Optional<org.apache.parquet.schema.Type> mapValueType(org.apache.parquet.schema.Type parquetType)
    {
        if (parquetType.isPrimitive()) {
            return Optional.empty();
        }
        GroupType keyValue = parquetType.asGroupType();
        while (keyValue.getFieldCount() == 1) {
            org.apache.parquet.schema.Type child = keyValue.getType(0);
            if (child.isPrimitive()) {
                return Optional.empty();
            }
            keyValue = child.asGroupType();
        }
        if (keyValue.getFieldCount() != 2) {
            return Optional.empty();
        }
        return Optional.of(keyValue.getType(1));
    }

    /**
     * A UUID read as varbinary: the 16 raw bytes of a FIXED_LEN_BYTE_ARRAY column, or the 36 characters of its
     * canonical text form in a string column.
     */
    private static Block uuidsFromBinary(Block block)
    {
        int positionCount = block.getPositionCount();
        BlockBuilder builder = UuidType.UUID.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            switch (value.length()) {
                // Trino's uuid value is the UUID's big-endian bytes, the same bytes parquet stores
                case 16 -> UuidType.UUID.writeSlice(builder, value);
                case 36 -> UuidType.UUID.writeSlice(builder, javaUuidToTrinoUuid(parseUuid(value)));
                default -> throw new TrinoException(HUDI_BAD_DATA, "Expected a UUID as 16 bytes or as its 36-character text form, but found %s bytes".formatted(value.length()));
            }
        }
        return builder.build();
    }

    private static UUID parseUuid(Slice value)
    {
        String text = value.toStringUtf8();
        try {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(HUDI_BAD_DATA, "Invalid UUID: " + text, e);
        }
    }

    /**
     * A VECTOR read as varbinary, see {@link #writeVector}.
     */
    private static Block vectorsFromBinary(ArrayType arrayType, Block block)
    {
        int positionCount = block.getPositionCount();
        ArrayBlockBuilder builder = arrayType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            writeVector(arrayType, VARBINARY.getSlice(block, position), builder);
        }
        return builder.build();
    }

    /**
     * Appends a VECTOR's bytes to {@code output} as one array of {@code arrayType}: the elements back to back in
     * little-endian order (HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER), which is also the byte order of
     * {@link Slice}'s accessors.
     */
    static void writeVector(ArrayType arrayType, Slice value, BlockBuilder output)
    {
        Type elementType = arrayType.getElementType();
        int elementSize = vectorElementSize(elementType);
        if (value.length() % elementSize != 0) {
            throw new TrinoException(HUDI_BAD_DATA, "A vector of %s needs a multiple of %s bytes, but found %s bytes".formatted(elementType, elementSize, value.length()));
        }
        int elementCount = value.length() / elementSize;
        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            for (int element = 0; element < elementCount; element++) {
                writeVectorElement(elementType, value, element * elementSize, elementBuilder);
            }
        });
    }

    private static int vectorElementSize(Type elementType)
    {
        if (REAL.equals(elementType)) {
            return Float.BYTES;
        }
        if (DOUBLE.equals(elementType)) {
            return Double.BYTES;
        }
        if (TINYINT.equals(elementType)) {
            return Byte.BYTES;
        }
        throw new IllegalArgumentException("Not a vector element type: " + elementType);
    }

    private static void writeVectorElement(Type elementType, Slice value, int offset, BlockBuilder elementBuilder)
    {
        if (REAL.equals(elementType)) {
            REAL.writeLong(elementBuilder, floatToRawIntBits(value.getFloat(offset)));
        }
        else if (DOUBLE.equals(elementType)) {
            DOUBLE.writeDouble(elementBuilder, value.getDouble(offset));
        }
        else {
            TINYINT.writeLong(elementBuilder, value.getByte(offset));
        }
    }

    private static Block convertArrayElements(Block block, Function<Block, Block> elementConversion)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        int positionCount = columnarArray.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            offsets[position + 1] = offsets[position] + columnarArray.getLength(position);
        }
        return ArrayBlock.fromElementBlock(positionCount, valueIsValid(block), offsets, elementConversion.apply(columnarArray.getElementsBlock()));
    }

    private static Block convertMapValues(Block block, MapType mapType, Function<Block, Block> valueConversion)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
        int positionCount = columnarMap.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            offsets[position + 1] = offsets[position] + columnarMap.getEntryCount(position);
        }
        return MapBlock.fromKeyValueBlock(
                valueIsValid(block),
                offsets,
                positionCount,
                columnarMap.getKeysBlock(),
                valueConversion.apply(columnarMap.getValuesBlock()),
                mapType);
    }

    private static Block convertRowFields(Block block, List<Optional<Function<Block, Block>>> fieldConversions)
    {
        // Not null-suppressed: each field block is as long as the row block, and null rows hold null fields
        List<Block> fields = RowBlock.getRowFieldsFromBlock(block);
        Block[] convertedFields = new Block[fields.size()];
        for (int field = 0; field < fields.size(); field++) {
            Block fieldBlock = fields.get(field);
            convertedFields[field] = fieldConversions.get(field)
                    .map(conversion -> conversion.apply(fieldBlock))
                    .orElse(fieldBlock);
        }
        return RowBlock.fromNotNullSuppressedFieldBlocks(block.getPositionCount(), valueIsValid(block), convertedFields);
    }

    private static Optional<long[]> valueIsValid(Block block)
    {
        if (!block.mayHaveNull()) {
            return Optional.empty();
        }
        int positionCount = block.getPositionCount();
        long[] valueIsValid = Bitmap.allocateWords(positionCount, true);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                Bitmap.clear(valueIsValid, 0, position);
            }
        }
        return Optional.of(valueIsValid);
    }
}
