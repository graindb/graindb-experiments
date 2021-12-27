package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNodeSequence;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;

import java.io.Serializable;

public class ScanAllNodes extends Operator implements Serializable {

    @Getter String nodeName;
    protected int type;

    protected ValueVector outVector;
    protected int numFullVectors;
    protected int finalOffset;
    protected int finalSize;
    protected DataChunk dataChunk;

    public ScanAllNodes(NodeVariable nodeVariable) {
        this.nodeName = nodeVariable.getVariableName();
        this.type = nodeVariable.getType();
        this.operatorName = "VERTEXSCAN (" + nodeName + ")";
    }

    @Override
    public void initFurther(Graph graph) {
        var highestOffset = graph.getNumNodesPerType()[type] - 1;
        this.numFullVectors = (int) highestOffset / ValueVector.DEFAULT_VECTOR_SIZE;
        this.finalOffset = numFullVectors * ValueVector.DEFAULT_VECTOR_SIZE;
        this.finalSize = (int) (highestOffset - finalOffset) + 1;
        this.dataChunk = new DataChunk(ValueVector.DEFAULT_VECTOR_SIZE);
        this.dataChunk.setSizeAndNumTrueValuesInSelector(ValueVector.DEFAULT_VECTOR_SIZE);
        allocateOutVector();
        this.dataChunk.append(outVector);
        this.dataChunks = new DataChunks();
        this.dataChunks.append(dataChunk);
        this.dataChunks.addVartoPos(nodeName, 0 /* dataChunkPos*/, 0 /* vectorPos */);
    }

    protected void allocateOutVector() {
        this.outVector = new VectorNodeSequence();
    }

    @Override
    public void processNewDataChunks() {}

    @Override
    public void reset() {
        this.dataChunk.setSizeAndNumTrueValuesInSelector(ValueVector.DEFAULT_VECTOR_SIZE);
    }

    @Override
    public void execute() {
        for (var i = 0; i < numFullVectors; i++) {
            outVector.setNodeOffset(i * ValueVector.DEFAULT_VECTOR_SIZE);
            dataChunk.resetSelector();
            next.processNewDataChunks();
        }
        outVector.setNodeOffset(finalOffset);
        dataChunk.setSizeAndNumTrueValuesInSelector(finalSize);
        dataChunk.resetSelector();
        next.processNewDataChunks();
        notifyAllDone();
    }
}
