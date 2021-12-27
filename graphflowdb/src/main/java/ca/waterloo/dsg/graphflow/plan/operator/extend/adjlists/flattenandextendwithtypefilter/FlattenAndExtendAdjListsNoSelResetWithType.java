package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextendwithtypefilter;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;

import java.io.Serializable;

public class FlattenAndExtendAdjListsNoSelResetWithType
    extends FlattenAndExtendAdjListsWithType implements Serializable {

    public FlattenAndExtendAdjListsNoSelResetWithType(AdjListDescriptor ALD, int typeFilter) {
        super(ALD, typeFilter);
    }

    @Override
    public void processNewDataChunks() {
        it.init();
        var size = inDataChunk.size();
        for (inDataChunk.currentPos = 0; inDataChunk.currentPos < size; inDataChunk.currentPos++) {
            if (selector[inDataChunk.currentPos]) {
                index.readAdjList(outVector, it.getNextNodeOffset());
                var dataChunkSize = outVector.filter(typeFilter);
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
