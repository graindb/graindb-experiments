package ca.waterloo.dsg.graphflow.plan.operator.filter;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

public class Filter extends Operator {

    private final Expression predicate;
    protected ExpressionEvaluator evaluator;
    protected DataChunk lastExtendedToDataChunk;
    protected boolean[] selector;

    public Filter(Expression predicate) {
        this.predicate = predicate;
        operatorName = "Filter: " + predicate.getPrintableExpression();
    }

    @Override
    protected void initFurther(Graph graph) {
        evaluator = predicate.getEvaluator(dataChunks);
        var predicateVars = predicate.getDependentVariableNames();
        lastExtendedToDataChunk = dataChunks.getLastExtendedToDataChunk(predicateVars);
        selector = lastExtendedToDataChunk.getSelector();
    }

    @Override
    public void processNewDataChunks() {
        var result = evaluator.evaluate();
        var numTrueValuesInSelector = 0;
        for (var i = 0; i < lastExtendedToDataChunk.size(); i++) {
            selector[i] &= result.vector.getBoolean(i);
            if (selector[i]) {
                numTrueValuesInSelector++;
            }
        }
        if (numTrueValuesInSelector == 0) return;
        lastExtendedToDataChunk.setNumTrueValuesInSelector(numTrueValuesInSelector);
        next.processNewDataChunks();
    }
}
