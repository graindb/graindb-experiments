package ca.waterloo.dsg.graphflow.datachunk.vectors.iterator;

public interface VectorNodesOrAdjEdgesIterator {

    void init();
    void moveCursor();
    int getNextNodeOffset();
    default int getNextRelBucketOffset() { throw new UnsupportedOperationException(); }
}
