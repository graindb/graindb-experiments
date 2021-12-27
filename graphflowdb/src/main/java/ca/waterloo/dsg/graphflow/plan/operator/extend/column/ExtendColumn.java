package ca.waterloo.dsg.graphflow.plan.operator.extend.column;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols.VectorAdjCols;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjcols.VectorAdjCols.VectorAdjColsCopied;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.columnadjlistindex.ColumnAdjListIndex;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class ExtendColumn extends Operator {

    protected final AdjListDescriptor ALD;

    protected DataChunk dataChunk;
    protected boolean[] selector;
    protected ValueVector inVector;
    protected VectorAdjColsCopied outVector;

    protected ColumnAdjListIndex index;

    public ExtendColumn(AdjListDescriptor ALD) {
        this.ALD = ALD;
        this.operatorName = String.format("%s: (%s)*%s(%s) using %s adjList",
            getClass().getSimpleName(), ALD.getBoundNodeVariable().getVariableName(),
            (Direction.FORWARD == ALD.getDirection() ? "->" : "<-"), ALD.getToNodeVariable().
                getVariableName(), ALD.getDirection());
    }

    @Override
    protected void initFurther(Graph graph) {
        var labelIdx = graph.getGraphCatalog()
            .getTypeToColumnAdjListIndexLabelsMapInDirection(ALD.getDirection())
            .get(ALD.getBoundNodeVariable().getType())
            .indexOf(ALD.getRelVariable().getLabel());
        index = graph.getAdjListIndexes().getColumnAdjListIndexForDirection(ALD.getDirection(),
            ALD.getBoundNodeVariable().getType(), labelIdx);

        dataChunk = dataChunks.getDataChunk(ALD.getBoundNodeVariable().getVariableName());
        selector = dataChunk.getSelector();
        var boundVarName = ALD.getBoundNodeVariable().getVariableName();
        inVector = dataChunks.getValueVector(boundVarName);

        var dataChunkPos = dataChunks.getDataChunkPos(ALD.getBoundNodeVariable().getVariableName());
        var valueVectorPos = dataChunk.getNumValueVectors();
        dataChunks.addVartoPos(ALD.getToNodeVariable().getVariableName(), dataChunkPos, valueVectorPos);
        dataChunks.addVartoPos(ALD.getRelVariable().getVariableName(), dataChunkPos, valueVectorPos);
        allocateOutVector(graph, (int) graph.getNumRels());
        dataChunk.append(outVector);
    }

    @Override
    public void processNewDataChunks() {
        var numTrueValuesInSelector = 0;
        for (var i = 0; i < dataChunk.size(); i++) {
            if (selector[i]) {
                var nbrOffset = index.getNodeOffset(inVector.getNodeOffset(i));
                selector[i] = DataType.NULL_INTEGER != nbrOffset;
                if (selector[i]) {
                    numTrueValuesInSelector++;
                    outVector.setNodeOffset(nbrOffset, i);
                }
            }
        }
        if (0 == numTrueValuesInSelector) return;
        dataChunk.setNumTrueValuesInSelector(numTrueValuesInSelector);
        next.processNewDataChunks();
    }

    protected void allocateOutVector(Graph graph, int capacity) {
        outVector = (VectorAdjColsCopied) VectorAdjCols.make(graph.getGraphCatalog(),
            ALD.getRelVariable().getLabel(), ALD.getDirection(), capacity);
    }
}
