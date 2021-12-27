package ca.waterloo.dsg.graphflow.datachunk.vectors.node;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;

public class VectorNodeSequence extends ValueVector {

    private int nodeType;
    private int startNodeOffset;

    @Override
    public int getNodeType(int pos) {
        return nodeType;
    }

    @Override
    public int getNodeOffset(int pos) {
        return startNodeOffset + pos;
    }

    @Override
    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public void setNodeOffset(int nodeOffset) {
        this.startNodeOffset = nodeOffset;
    }

    @Override
    public VectorNodesOrAdjEdgesIterator getIterator() {
        return new VectorNodeSequenceIterator(this);
    }

    public static class VectorNodeSequenceIterator implements VectorNodesOrAdjEdgesIterator {

        private final VectorNodeSequence vector;
        private int pos;

        VectorNodeSequenceIterator(VectorNodeSequence vector) {
            this.vector = vector;
        }

        @Override
        public void init() {
            this.pos = 0;
        }

        @Override
        public int getNextNodeOffset() {
            return vector.startNodeOffset + pos;
        }

        @Override
        public void moveCursor() {
            pos++;
        }
    }
}
