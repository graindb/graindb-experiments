package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;

import java.io.Serializable;

public class ScanAllNodesNoSelReset extends ScanAllNodes implements Serializable {

    public ScanAllNodesNoSelReset(NodeVariable nodeVariable) {
        super(nodeVariable);
    }

    @Override
    public void execute() {
        for (var i = 0; i < numFullVectors; i++) {
            outVector.setNodeOffset(i * ValueVector.DEFAULT_VECTOR_SIZE);
            next.processNewDataChunks();
        }
        outVector.setNodeOffset(finalOffset);
        dataChunk.setSizeAndNumTrueValuesInSelector(finalSize);
        next.processNewDataChunks();
        notifyAllDone();
    }
}
