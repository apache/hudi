package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

public class ColumnarGroupArrayData implements ArrayData {

    WritableColumnVector vector;
    int rowId;

    public ColumnarGroupArrayData(WritableColumnVector vector, int rowId) {
        this.vector = vector;
        this.rowId = rowId;
    }

    @Override
    public int size() {
        if (vector == null) {
            return 0;
        }

        if (vector instanceof HeapRowColumnVector) {
            // assume all fields have the same size
            if (((HeapRowColumnVector)vector).vectors == null || ((HeapRowColumnVector)vector).vectors.length == 0) {
                return 0;
            }
            return ((HeapArrayVector)((HeapRowColumnVector)vector).vectors[0]).getArray(rowId).size();
        }
        throw new UnsupportedOperationException(vector.getClass().getName() + " is not supported. Supported vector types: HeapRowColumnVector");
    }

    @Override
    public boolean isNullAt(int index) {
        if (vector == null) {
            return true;
        }

        if (vector instanceof HeapRowColumnVector) {
            return ((HeapRowColumnVector) vector).vectors == null;
        }

        throw new UnsupportedOperationException(vector.getClass().getName() + " is not supported. Supported vector types: HeapRowColumnVector");
    }

    @Override
    public boolean getBoolean(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public byte getByte(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public short getShort(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public int getInt(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public long getLong(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public float getFloat(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public double getDouble(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public StringData getString(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public DecimalData getDecimal(int index, int precision, int scale) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public TimestampData getTimestamp(int index, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public <T> RawValueData<T> getRawValue(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public byte[] getBinary(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public ArrayData getArray(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public MapData getMap(int index) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public RowData getRow(int index, int numFields) {
        return new ColumnarGroupRowData((HeapRowColumnVector)vector, rowId, index);
    }

    @Override
    public boolean[] toBooleanArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public short[] toShortArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public int[] toIntArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public long[] toLongArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public float[] toFloatArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public double[] toDoubleArray() {
        throw new UnsupportedOperationException("Not support the operation!");
    }

}
