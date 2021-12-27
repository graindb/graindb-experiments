package ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextendwithtypefilter;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;

import java.io.Serializable;

public class ExtendAdjListsWithType extends FlattenAndExtendAdjLists implements Serializable {

    protected final int typeFilter;

    public ExtendAdjListsWithType(AdjListDescriptor ALD, int typeFilter) {
        super(ALD);
        this.typeFilter = typeFilter;
    }

    @Override
    public void processNewDataChunks() {
        if (selector[inDataChunk.currentPos]) {
            var fromNodeOffset = inVector.getNodeOffset(inDataChunk.currentPos);
            index.readAdjList(outVector, fromNodeOffset);
            var dataChunkSize = outVector.filter(typeFilter);
            if (0 == dataChunkSize) return;
            outDataChunk.setSizeAndNumTrueValuesInSelector(dataChunkSize);
            outDataChunk.resetSelector();
            next.processNewDataChunks();
        }
    }
}
