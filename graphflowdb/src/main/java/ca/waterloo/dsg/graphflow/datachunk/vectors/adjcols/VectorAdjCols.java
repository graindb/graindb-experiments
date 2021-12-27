package ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols.VectorAdjCols.VectorAdjColsCopied.VectorAdjColsCopiedSingleType;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;

public abstract class VectorAdjCols extends ValueVector {

    public static VectorAdjCols make(GraphCatalog catalog, int label, Direction direction,
        int capacity) {
        var nbrTypes = catalog.getLabelToNbrTypeMapInDirection(direction).get(label);
        return new VectorAdjColsCopiedSingleType(nbrTypes.get(0), capacity);
    }

    /**
     * Vector AdjCols Copied.
     *
     * Used to extend from NodeSequence vector. Copy the edges from the column to the
     * intermediate arrays.
     */
    public abstract static class VectorAdjColsCopied extends VectorAdjCols {

        public static class VectorAdjColsIterator implements VectorNodesOrAdjEdgesIterator {

            VectorAdjColsCopied vector;
            public int pos;

            public VectorAdjColsIterator(VectorAdjColsCopied vector) {
                this.vector = vector;
            }

            @Override
            public void init() {
                pos = 0;
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

        @Override
        public VectorAdjColsIterator getIterator() {
            return new VectorAdjColsCopied.VectorAdjColsIterator(this);
        }

        protected int[] nodeOffsets;

        @Override
        public int getNodeOffset(int pos) {
            return nodeOffsets[pos];
        }

        @Override
        public void setNodeOffset(int offset, int pos) {
            nodeOffsets[pos] = offset;
        }

        /**
         * Vector AdjCols Copied Single Type.
         */
        public static class VectorAdjColsCopiedSingleType extends VectorAdjColsCopied {

            public VectorAdjColsCopiedSingleType(int nodeType, int capacity) {
                this.nodeType = nodeType;
                this.nodeOffsets = new int[capacity];
            }

            protected int nodeType;

            public int getNodeType(int pos) { return nodeType; }
        }
    }
}
