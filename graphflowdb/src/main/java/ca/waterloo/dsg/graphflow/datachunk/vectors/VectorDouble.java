package ca.waterloo.dsg.graphflow.datachunk.vectors;

public class VectorDouble extends ValueVector {

    protected double[] values;

    protected VectorDouble() {}

    protected VectorDouble(int capacity) {
        this.values = new double[capacity];
    }

    @Override
    public double getDouble(int pos) {
        return values[pos];
    }

    @Override
    public void set(int pos, double value) {
        this.values[pos] = value;
    }

    @Override
    public void set(double[] values) {
        this.values = values;
    }

    public static class VectorDoubleWithOffset extends VectorDouble {

        private int posOffset;

        @Override
        public void setPosOffset(int posOffset) {
            this.posOffset = posOffset;
        }

        @Override
        public double getDouble(int pos) {
            return values[pos + posOffset];
        }
    }
}
