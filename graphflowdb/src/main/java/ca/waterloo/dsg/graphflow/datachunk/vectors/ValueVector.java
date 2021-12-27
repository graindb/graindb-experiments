package ca.waterloo.dsg.graphflow.datachunk.vectors;

import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorBoolean.VectorBooleanWithOffset;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorDouble.VectorDoubleWithOffset;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorInt.VectorIntWithOffset;
import ca.waterloo.dsg.graphflow.datachunk.vectors.VectorString.VectorStringWithOffset;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class ValueVector {

    public final static int DEFAULT_VECTOR_SIZE = 1024;

    public static ValueVector make(DataType dataType) {
        return make(dataType, DEFAULT_VECTOR_SIZE * 1000);
    }

    public static ValueVector make(DataType dataType, int capacity) {
        switch (dataType) {
            case INT:
                return new VectorInt(capacity);
            case DOUBLE:
                return new VectorDouble(capacity);
            case BOOLEAN:
                return new VectorBoolean(capacity);
            case STRING:
                return new VectorString(capacity);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static ValueVector makeWithOffset(DataType dataType) {
        switch (dataType) {
            case INT:
                return new VectorIntWithOffset();
            case DOUBLE:
                return new VectorDoubleWithOffset();
            case BOOLEAN:
                return new VectorBooleanWithOffset();
            case STRING:
                return new VectorStringWithOffset();
            default:
                throw new IllegalArgumentException();
        }
    }

    /*********************************************************************************************
     *                                       GETTER METHODS                                      *
     *********************************************************************************************/

    public VectorNodesOrAdjEdgesIterator getIterator() { throw new UnsupportedOperationException(); }

    public int filter(int type) { throw new UnsupportedOperationException(); }

    public int getIntsArrayOffset() { return 0; }

    public int getInt(int pos) { throw new UnsupportedOperationException(); }

    public boolean getBoolean(int pos) { throw new UnsupportedOperationException(); }

    public double getDouble(int pos) { throw new UnsupportedOperationException(); }

    public String getString(int pos) { throw new UnsupportedOperationException(); }

    public int getNodeType(int pos) { throw new UnsupportedOperationException(); }

    public int getNodeOffset(int pos) { throw new UnsupportedOperationException(); }

    public int getRelBucketOffset(int posOrNodeOffset) { throw new UnsupportedOperationException(); }

    /*********************************************************************************************
     *                                       SETTER METHODS                                      *
     *********************************************************************************************/

    public int[] getInts() { throw new UnsupportedOperationException(); }

    public int[] getDoubles() { throw new UnsupportedOperationException(); }

    public int[] getBooleans() { throw new UnsupportedOperationException(); }

    public int[] getStrings() { throw new UnsupportedOperationException(); }

    public int[] getNodeOffsets() { throw new UnsupportedOperationException(); }

    public int[] getNodeTypes() { throw new UnsupportedOperationException(); }

    public void set(int[] values) { throw new UnsupportedOperationException(); }

    public void set(double[] values) { throw new UnsupportedOperationException(); }

    public void set(String[] values) { throw new UnsupportedOperationException(); }

    public void set(boolean[] values) { throw new UnsupportedOperationException(); }

    // in NodeSequence.
    public void setNodeType(int nodeType) { throw new UnsupportedOperationException(); }
    public void setNodeOffset(int nodeOffset) { throw new UnsupportedOperationException(); }

    // in VectorAdjCols.
    public void setNodeOffset(int nodeOffset, int pos) { throw new UnsupportedOperationException(); }
    public void setNodeType(int nodeType, int pos) { throw new UnsupportedOperationException(); }

    // in VectorAdjEdges.
    public int set(byte[] bytesArray, int[] intsArray, int nodeOffset) {
        throw new UnsupportedOperationException();
    }

    public void set(int pos, int value) { throw new UnsupportedOperationException(); }

    public void set(int pos, double value) { throw new UnsupportedOperationException(); }

    public void set(int pos, String value) { throw new UnsupportedOperationException(); }

    public void set(int pos, boolean value) { throw new UnsupportedOperationException(); }

    public void setPosOffset(int posOffset) { throw new UnsupportedOperationException(); }
}
