package ca.waterloo.dsg.graphflow.plan.operator.filter;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;

public class FilterNoAnd extends Filter {

    public FilterNoAnd(Expression predicate) {
        super(predicate);
    }

    @Override
    public void processNewDataChunks() {
        var result = evaluator.evaluate();
        var numTrueValuesInSelector = 0;
        for (var i = 0; i < lastExtendedToDataChunk.size(); i++) {
            selector[i] = result.vector.getBoolean(i);
            if (selector[i]) {
                numTrueValuesInSelector++;
            }
        }
        if (numTrueValuesInSelector == 0) return;
        lastExtendedToDataChunk.setNumTrueValuesInSelector(numTrueValuesInSelector);
        next.processNewDataChunks();
    }
}
