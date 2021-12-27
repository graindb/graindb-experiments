package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNode;
import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNodeSequence;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;

import java.io.Serializable;

public class ScanRangeNodes extends ScanAllNodes implements Serializable {

    int startOffset;
    int endOffset;
    int step;

    public ScanRangeNodes(NodeVariable nodeVariable, int startOffset, int endOffset, int step) {
        super(nodeVariable);
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.step = step;
        this.operatorName = "VERTEXSCAN (" + nodeName + ", start=" + startOffset + ", end=" +
            endOffset + ", step=" + step + ")";
    }

    @Override
    protected void allocateOutVector() {
        if (1 == step) {
            outVector = new VectorNodeSequence();
            outVector.setNodeOffset(startOffset);
            dataChunk.setSizeAndNumTrueValuesInSelector(endOffset - startOffset);
        } else {
            outVector = new VectorNode((endOffset - startOffset) / step);
            var i = 0;
            for (var offset = startOffset; offset < endOffset; offset += step) {
                outVector.setNodeOffset(i++, offset);
            }
            dataChunk.setSizeAndNumTrueValuesInSelector((endOffset - startOffset) / step);
        }
    }

    @Override
    public void execute() {
        next.processNewDataChunks();
        notifyAllDone();
    }
}
