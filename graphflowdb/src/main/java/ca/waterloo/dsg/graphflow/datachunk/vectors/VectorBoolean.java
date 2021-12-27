package ca.waterloo.dsg.graphflow.datachunk.vectors;

public class VectorBoolean extends ValueVector {

    protected boolean[] values;

    protected VectorBoolean() {}

    protected VectorBoolean(int capacity) {
        this.values = new boolean[capacity];
    }

    @Override
    public boolean getBoolean(int pos) {
        return values[pos];
    }

    @Override
    public void set(int pos, boolean value) {
        this.values[pos] = value;
    }

    @Override
    public void set(boolean[] values) {
        this.values = values;
    }

    public static class VectorBooleanWithOffset extends VectorBoolean {

        private int posOffset;

        @Override
        public void setPosOffset(int posOffset) {
            this.posOffset = posOffset;
        }

        @Override
        public boolean getBoolean(int pos) {
            return values[pos + posOffset];
        }
    }
}
