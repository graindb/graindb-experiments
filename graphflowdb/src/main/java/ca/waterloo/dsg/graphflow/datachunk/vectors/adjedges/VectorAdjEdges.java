package ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.util.Configuration;

public abstract class VectorAdjEdges extends ValueVector {

    protected byte[] bytesArray;
    protected int[] intsArray;
    protected int nodeOffset;
    protected int relIdx;
    protected int relsOffsetStart, relsOffsetEnd;

    protected int intsArrayOffset;
    protected int intsArrayLabelEndIdx;

    @Override
    public int set(byte[] bytesArray, int[] intsArray, int nodeOffset) {
        this.bytesArray = bytesArray;
        this.intsArray = intsArray;
        this.nodeOffset = nodeOffset;
        relIdx = Configuration.getDefaultAdjListGroupingSize() + 1;
        relsOffsetStart = intsArray[nodeOffset];
        relsOffsetEnd = intsArray[nodeOffset + 1];
        setIntsArrayOffsets();
        return relsOffsetEnd - relsOffsetStart;
    }

    @Override
    public int getIntsArrayOffset() {
        return intsArrayOffset;
    }

    protected abstract void setIntsArrayOffsets();
}
