package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdges;
import ca.waterloo.dsg.graphflow.datachunk.vectors.adjedges.VectorAdjEdgesFactory;
import ca.waterloo.dsg.graphflow.datachunk.vectors.iterator.VectorNodesOrAdjEdgesIterator;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.defaultadjlistindex.DefaultAdjListIndex;

import java.io.Serializable;

public class FlattenAndExtendAdjLists extends Operator implements Serializable {

    private final AdjListDescriptor ALD;

    protected DataChunk inDataChunk;
    protected DataChunk outDataChunk;
    protected ValueVector inVector;
    protected VectorAdjEdges outVector;
    protected boolean[] selector;
    protected VectorNodesOrAdjEdgesIterator it;

    protected DefaultAdjListIndex index;

    public FlattenAndExtendAdjLists(AdjListDescriptor ALD) {
        this.ALD = ALD;
        this.operatorName = String.format("%s: (%s)*%s(%s) using %s adjList",
            getClass().getSimpleName(), ALD.getBoundNodeVariable().getVariableName(),
            (Direction.FORWARD == ALD.getDirection() ? "->" : "<-"), ALD.getToNodeVariable().
                getVariableName(), ALD.getDirection());
    }

    @Override
    protected void initFurther(Graph graph) {
        var labelIdx = graph.getGraphCatalog()
                            .getTypeToDefaultAdjListIndexLabelsMapInDirection(ALD.getDirection())
                            .get(ALD.getBoundNodeVariable().getType())
                            .indexOf(ALD.getRelVariable().getLabel());
        index = graph.getAdjListIndexes().getDefaultAdjListIndexForDirection(ALD.getDirection(),
            ALD.getBoundNodeVariable().getType(), labelIdx);

        var boundVarName = ALD.getBoundNodeVariable().getVariableName();
        inDataChunk = dataChunks.getDataChunk(boundVarName);
        inVector = dataChunks.getValueVector(boundVarName);
        it = inVector.getIterator();
        selector = inDataChunk.getSelector();
        dataChunks.setAsFlat(boundVarName);

        var nbrVarName = ALD.getToNodeVariable().getVariableName();
        var relName = ALD.getRelVariable().getVariableName();
        var outDataChunkIdx = dataChunks.size();
        dataChunks.addVartoPos(nbrVarName, outDataChunkIdx, 0 /* vectorPos */);
        dataChunks.addVartoPos(relName, outDataChunkIdx, 0 /* vectorPos */);
        outVector = VectorAdjEdgesFactory.make(graph.getGraphCatalog(),
            ALD.getRelVariable().getLabel(), ALD.getDirection());
        outDataChunk = new DataChunk((int) graph.getNumRelsPerLabel()[ALD.getLabel()]);
        outDataChunk.append(outVector);
        dataChunks.append(outDataChunk);
    }

    @Override
    public void processNewDataChunks() {
        it.init();
        var size = inDataChunk.size();
        for (inDataChunk.currentPos = 0; inDataChunk.currentPos < size; inDataChunk.currentPos++) {
            if (selector[inDataChunk.currentPos]) {
                var dataChunkSize = index.readAdjList(outVector, it.getNextNodeOffset());
                if (0 == dataChunkSize) {
                    it.moveCursor();
                    continue;
                }
                outDataChunk.setSizeAndNumTrueValuesInSelector(dataChunkSize);
                outDataChunk.resetSelector();
                next.processNewDataChunks();
            }
            it.moveCursor();
        }
        inDataChunk.currentPos = -1;
    }
}
