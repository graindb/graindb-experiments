package ca.waterloo.dsg.graphflow.datachunk.vectors.node;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;

public class VectorNode extends ValueVector {

    private int nodeType;
    private int[] nodeOffsets;

    public VectorNode(int capacity) {
        nodeOffsets = new int[capacity];
    }

    @Override
    public int getNodeType(int pos) {
        return nodeType;
    }

    @Override
    public int getNodeOffset(int pos) {
        return nodeOffsets[pos];
    }

    @Override
    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public void setNodeOffset(int pos, int nodeOffsets) {
        this.nodeOffsets[pos] = nodeOffsets;
    }

    @Override
    public int[] getNodeOffsets() {
        return nodeOffsets;
    }

    @Override
    public VectorNodesOrAdjEdgesIterator getIterator() {
        return new VectorNode.VectorNodesIterator(this);
    }

    public static class VectorNodesIterator implements VectorNodesOrAdjEdgesIterator {

        private final VectorNode vector;
        private int pos;

        VectorNodesIterator(VectorNode vector) {
            this.vector = vector;
        }

        @Override
        public void init() {
            this.pos = 0;
        }

        @Override
        public int getNextNodeOffset() {
            return vector.nodeOffsets[pos];
        }

        @Override
        public void moveCursor() {
            pos++;
        }
    }
}
