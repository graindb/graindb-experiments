package ca.waterloo.dsg.graphflow.datachunk.vectors;

public class VectorInt extends ValueVector {

    protected int[] values;

    protected VectorInt() {}

    protected VectorInt(int capacity) {
        this.values = new int[capacity];
    }

    @Override
    public int getInt(int pos) {
        return values[pos];
    }

    @Override
    public void set(int pos, int value) {
        this.values[pos] = value;
    }

    @Override
    public void set(int[] values) {
        this.values = values;
    }

    public static class VectorIntWithOffset extends VectorInt {

        private int posOffset;

        @Override
        public void setPosOffset(int posOffset) {
            this.posOffset = posOffset;
        }

        @Override
        public int getInt(int pos) {
            return values[pos + posOffset];
        }
    }
}
