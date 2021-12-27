package ca.waterloo.dsg.graphflow.datachunk.vectors;

public class VectorString extends ValueVector {

    protected String[] values;

    protected VectorString() {}

    protected VectorString(int capacity) {
        this.values = new String[capacity];
    }

    @Override
    public String getString(int pos) {
        return values[pos];
    }

    @Override
    public void set(int pos, String value) {
        this.values[pos] = value;
    }

    @Override
    public void set(String[] values) {
        this.values = values;
    }

    public static class VectorStringWithOffset extends VectorString {

        private int posOffset;

        @Override
        public void setPosOffset(int posOffset) {
            this.posOffset = posOffset;
        }

        @Override
        public String getString(int pos) {
            return values[pos + posOffset];
        }
    }
}
