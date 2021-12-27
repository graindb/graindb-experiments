package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.node.VectorNodeSequence;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.Getter;

public class ScanSingleNode extends Operator {

    @Getter int offset;
    String nodeName;

    public ScanSingleNode(NodeVariable nodeVariable, int offset) {
        this.nodeName = nodeVariable.getVariableName();
        this.operatorName = "VERTEXSCAN (" + nodeVariable + ", ID=" + offset + ")";
        this.offset = offset;
    }

    @Override
    public void initFurther(Graph graph) {
        var outVector = new VectorNodeSequence();
        outVector.setNodeOffset(offset);
        var dataChunk = new DataChunk(1);
        dataChunk.setSizeAndNumTrueValuesInSelector(1);
        dataChunk.append(outVector);
        this.dataChunks = new DataChunks();
        this.dataChunks.append(dataChunk);
        this.dataChunks.addVartoPos(nodeName, 0 /* dataChunkPos*/, 0 /* vectorPos */);
    }

    @Override
    public void processNewDataChunks() {}

    @Override
    public void execute() {
        next.processNewDataChunks();
        notifyAllDone();
    }
}
