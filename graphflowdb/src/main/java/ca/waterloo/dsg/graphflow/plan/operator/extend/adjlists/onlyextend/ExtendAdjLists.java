package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;

import java.io.Serializable;

public class ExtendAdjLists extends FlattenAndExtendAdjLists implements Serializable {

    public ExtendAdjLists(AdjListDescriptor ALD) {
        super(ALD);
    }

    @Override
    public void processNewDataChunks() {
        if (selector[inDataChunk.currentPos]) {
            var fromNodeOffset = inVector.getNodeOffset(inDataChunk.currentPos);
            var dataChunkSize = index.readAdjList(outVector, fromNodeOffset);
            if (0 == dataChunkSize) return;
            outDataChunk.setSizeAndNumTrueValuesInSelector(dataChunkSize);
            outDataChunk.resetSelector();
            next.processNewDataChunks();
        }
    }
}
