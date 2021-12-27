package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextendwithtypefilter;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;

import java.io.Serializable;

public class ExtendAdjListsNoSelResetWithType extends ExtendAdjListsWithType implements Serializable {

    public ExtendAdjListsNoSelResetWithType(AdjListDescriptor ALD, int typeFilter) {
        super(ALD, typeFilter);
    }

    @Override
    public void processNewDataChunks() {
        if (selector[inDataChunk.currentPos]) {
            var fromNodeOffset = inVector.getNodeOffset(inDataChunk.currentPos);
            index.readAdjList(outVector, fromNodeOffset);
            var dataChunkSize = outVector.filter(typeFilter);
            if (0 == dataChunkSize) return;
            outDataChunk.setSizeAndNumTrueValuesInSelector(dataChunkSize);
            next.processNewDataChunks();
        }
    }
}
