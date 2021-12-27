package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

import java.io.Serializable;

public class SinkCount extends Operator implements Serializable {

    DataChunk[] dataChunksAsLists;

    @Override
    protected void initFurther(Graph graph) {
        dataChunksAsLists = dataChunks.getUnflattenedDataChunks();
    }

    @Override
    public void processNewDataChunks() {
        long numTuplesInChunks = 1;
        for (var dataChunk : dataChunksAsLists) {
            if (dataChunk.currentPos == -1) {
                numTuplesInChunks *= dataChunk.getNumTrueValuesInSelector();
            }
        }
        numOutTuples += numTuplesInChunks;
    }

    @Override
    public void reset() {
        prev.getDataChunks().reset();
        numOutTuples = 0;
        prev.reset();
    }
}
