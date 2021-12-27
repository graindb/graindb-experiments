package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

public class NotExpression extends AbstractUnaryOperatorExpression {

    private Expression expression;

    public NotExpression(Expression expression) {
        super("", expression);
        this.expression = expression;
        setVariableName(getPrintableExpression());
    }

    public void verifyVariablesAndNormalize(Schema inputSchema,
        Schema matchGraphSchema, GraphCatalog catalog) {
        expression.verifyVariablesAndNormalize(inputSchema,
            matchGraphSchema, catalog);
        setDataType(DataType.BOOLEAN);
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var expressionEvaluator = expression.getEvaluator(dataChunks);
        if (ExpressionUtils.isExpressionOutputFlat(expression, dataChunks)) {
            return () -> {
                var result = expressionEvaluator.evaluate();
                result.vector.set(result.currentIdx, !result.vector.getBoolean(result.currentIdx));
                return result;
            };
        } else {
            return () -> {
                var result = expressionEvaluator.evaluate();
                for (var i = 0; i < result.size; i++) {
                    var val = !result.vector.getBoolean(i);
                    result.vector.set(i, val);
                }
                return result;
            };
        }
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + "NOT".hashCode();
        return hash;
    }
}
