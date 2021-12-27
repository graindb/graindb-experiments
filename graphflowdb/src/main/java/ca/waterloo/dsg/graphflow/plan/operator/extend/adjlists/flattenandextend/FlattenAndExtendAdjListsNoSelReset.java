package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;

import java.io.Serializable;

public class FlattenAndExtendAdjListsNoSelReset extends FlattenAndExtendAdjLists
    implements Serializable {

    public FlattenAndExtendAdjListsNoSelReset(AdjListDescriptor ALD) {
        super(ALD);
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
                next.processNewDataChunks();
            }
            it.moveCursor();
        }
        inDataChunk.currentPos = -1;
    }
}
