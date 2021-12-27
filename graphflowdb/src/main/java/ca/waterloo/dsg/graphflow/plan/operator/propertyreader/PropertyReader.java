package ca.waterloo.dsg.graphflow.plan.operator.propertyreader;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

public abstract class PropertyReader extends Operator {

    protected PropertyVariable variable;
    protected ValueVector inVector;
    protected ValueVector outVector;
    protected DataChunk dataChunk;
    protected boolean[] selector;
    protected VectorNodesOrAdjEdgesIterator it;

    public PropertyReader(PropertyVariable variable) {
        this.variable = variable;
        this.operatorName = getClass().getSimpleName() + ": " + variable.getVariableName();
    }

    @Override
    protected void initFurther(Graph graph) {
        var nodeOrRelVarName = variable.getNodeOrRelVariable().getVariableName();
        dataChunk = dataChunks.getDataChunk(nodeOrRelVarName);
        selector = dataChunk.getSelector();
        inVector = dataChunks.getValueVector(nodeOrRelVarName);
        it = inVector.getIterator();
        allocateOutVector();
        dataChunks.addVartoPos(variable.getVariableName(), dataChunks.getDataChunkPos(
            nodeOrRelVarName), dataChunk.getNumValueVectors());
        dataChunk.append(outVector);
    }

    protected void allocateOutVector() {
        outVector = ValueVector.make(variable.getDataType());
    }

    @Override
    public void processNewDataChunks() {
        readValues();
        next.processNewDataChunks();
    }

    protected abstract void readValues();
}
