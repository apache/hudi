package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

public class ColumnarGroupMapData implements MapData {

    WritableColumnVector keyVector;
    WritableColumnVector valueVector;
    int rowId;

    public ColumnarGroupMapData(WritableColumnVector keyVector, WritableColumnVector valueVector, int rowId) {
        this.keyVector = keyVector;
        this.valueVector = valueVector;
        this.rowId = rowId;
    }

    @Override
    public int size() {
        if (keyVector == null) {
            return 0;
        }

        if (keyVector instanceof HeapArrayVector) {
            return ((HeapArrayVector) keyVector).getArray(rowId).size();
        }
        throw new UnsupportedOperationException(keyVector.getClass().getName() + " is not supported. Supported vector types: HeapArrayVector");
    }

    @Override
    public ArrayData keyArray() {
        return ((HeapArrayVector) keyVector).getArray(rowId);
    }

    @Override
    public ArrayData valueArray() {
        if (valueVector instanceof HeapArrayVector) {
            return ((HeapArrayVector) keyVector).getArray(rowId);
        } else if (valueVector instanceof HeapArrayGroupColumnVector) {
            return ((HeapArrayGroupColumnVector) valueVector).getArray(rowId);
        }
        throw new UnsupportedOperationException(valueVector.getClass().getName() + " is not supported. Supported vector types: HeapArrayVector, HeapArrayGroupColumnVector");
    }
}
