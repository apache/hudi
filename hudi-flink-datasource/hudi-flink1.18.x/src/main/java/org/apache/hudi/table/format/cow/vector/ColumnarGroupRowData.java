package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

public class ColumnarGroupRowData implements RowData {

    HeapRowColumnVector vector;
    int rowId;
    int index;

    public ColumnarGroupRowData(HeapRowColumnVector vector, int rowId, int index) {
        this.vector = vector;
        this.rowId = rowId;
        this.index = index;
    }

    @Override
    public int getArity() {
        return vector.vectors.length;
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean isNullAt(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).isNullAt(index);
    }

    @Override
    public boolean getBoolean(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getBoolean(index);
    }

    @Override
    public byte getByte(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getByte(index);
    }

    @Override
    public short getShort(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getShort(index);
    }

    @Override
    public int getInt(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getInt(index);
    }

    @Override
    public long getLong(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getLong(index);
    }

    @Override
    public float getFloat(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getFloat(index);
    }

    @Override
    public double getDouble(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getDouble(index);
    }

    @Override
    public StringData getString(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getString(index);
    }

    @Override
    public DecimalData getDecimal(int pos, int i1, int i2) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getDecimal(index, i1, i2);
    }

    @Override
    public TimestampData getTimestamp(int pos, int i1) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getTimestamp(index, i1);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getRawValue(index);
    }

    @Override
    public byte[] getBinary(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getBinary(index);
    }

    @Override
    public ArrayData getArray(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getArray(index);
    }

    @Override
    public MapData getMap(int pos) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getMap(index);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return ((HeapArrayVector)(vector.vectors[pos])).getArray(rowId).getRow(index, numFields);
    }
}
