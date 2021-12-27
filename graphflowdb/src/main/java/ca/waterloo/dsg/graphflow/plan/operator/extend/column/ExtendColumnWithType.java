package ca.waterloo.dsg.graphflow.plan.operator.extend.column;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class ExtendColumnWithType extends ExtendColumn {

    protected int typeFilter;

    public ExtendColumnWithType(AdjListDescriptor ALD, int typeFilter) {
        super(ALD);
        this.typeFilter = typeFilter;
    }

    @Override
    public void processNewDataChunks() {
        var numTrueValuesInSelector = 0;
        for (var i = 0; i < dataChunk.size(); i++) {
            if (selector[i]) {
                var boundOffset = inVector.getNodeOffset(i);
                var nbrOffset = index.getNodeOffset(boundOffset);
                var nbrType = index.getNodeType(boundOffset);
                selector[i] = DataType.NULL_INTEGER != nbrOffset &&
                              typeFilter == nbrType;
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
}
