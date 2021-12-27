package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionResult;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.StringJoiner;

public abstract class BooleanConnectorExpression extends AbstractBinaryOperatorExpression {

    private enum BooleanConnector {
        AND,
        OR
    }

    private BooleanConnector boolConnector;

    public BooleanConnectorExpression(Expression leftExpression,
        Expression rightExpression, BooleanConnector boolConnector) {
        super("");
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
        this.boolConnector = boolConnector;
        setVariableName(getPrintableExpression());
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        throwExceptionIfSubexpressionDoesNotReturnABoolean(leftExpression);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        throwExceptionIfSubexpressionDoesNotReturnABoolean(rightExpression);
        setDataType(DataType.BOOLEAN);
    }

    private void throwExceptionIfSubexpressionDoesNotReturnABoolean(Expression subExpression) {
        if (DataType.BOOLEAN != subExpression.getDataType()) {
            throw new MalformedQueryException("Sub expression: "
                + subExpression.getPrintableExpression() + " of " + getClass().getSimpleName() +
                " has to return a BOOLEAN. Return type of subExpression: "
                + subExpression.getDataType()) ;
        }
    }

    @Override
    public String getPrintableExpression() {
        StringJoiner stringJoiner = new StringJoiner("");
        stringJoiner.add(leftExpression.getPrintableExpression());
        stringJoiner.add(" " + boolConnector.name() + " ");
        stringJoiner.add(rightExpression.getPrintableExpression());
        return stringJoiner.toString();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + boolConnector.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherBoolConExpr = (BooleanConnectorExpression) o;
        if (this.boolConnector != otherBoolConExpr.boolConnector) {
            return false;
        }
        if ((leftExpression.equals(otherBoolConExpr.leftExpression) &&
            rightExpression.equals(otherBoolConExpr.rightExpression)) ||
            (leftExpression.equals(otherBoolConExpr.rightExpression) &&
                rightExpression.equals(otherBoolConExpr.leftExpression))) {
            return true;
        }
        return false;
    }

    public static class ANDExpression extends BooleanConnectorExpression {

        public ANDExpression(Expression leftExpression, Expression rightExpression) {
            super(leftExpression, rightExpression, BooleanConnector.AND);
        }

        @Override
        public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
            var leftExprEval = leftExpression.getEvaluator(dataChunks);
            var rightExprEval = rightExpression.getEvaluator(dataChunks);
            var isLVectorFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
            var isRVectorFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
            var boolVector = ValueVector.make(DataType.BOOLEAN, 1024 * 40 /* temp */);
            var result = new ExpressionResult();
            result.vector = boolVector;
            if (isLVectorFlat && isRVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    boolVector.set(0, leftResult.vector.getBoolean(leftResult.currentIdx)
                        && rightResult.vector.getBoolean(rightResult.currentIdx));
                    result.size = 1;
                    result.currentIdx = 0;
                    return result;
                };
            } else if (!isLVectorFlat && isRVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    var rightValue = rightResult.vector.getBoolean(rightResult.currentIdx);
                    boolean val;
                    for (var i = 0; i < leftResult.size; i++) {
                        val = leftResult.vector.getBoolean(i) && rightValue;
                        boolVector.set(i, val);
                    }
                    result.size = leftResult.size;
                    return result;
                };
            } else if (isLVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    var lVal = leftResult.vector.getBoolean(leftResult.currentIdx);
                    boolean val;
                    for (var i = 0; i < rightResult.size; i++) {
                        val = lVal && rightResult.vector.getBoolean(i);
                        boolVector.set(i, val);
                    }
                    result.size = rightResult.size;
                    return result;
                };
            } else {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    boolean val;
                    for (var i = 0; i < rightResult.size; i++) {
                        val = leftResult.vector.getBoolean(i) && rightResult.vector.getBoolean(i);
                        boolVector.set(i, val);
                    }
                    result.size = rightResult.size;
                    return result;
                };
            }
        }
    }

    public static class ORExpression extends BooleanConnectorExpression {

        public ORExpression(Expression leftExpression, Expression rightExpression) {
            super(leftExpression, rightExpression, BooleanConnector.OR);
        }

        @Override
        public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
            var leftExprEval = leftExpression.getEvaluator(dataChunks);
            var rightExprEval = rightExpression.getEvaluator(dataChunks);
            var isLVectorFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
            var isRVectorFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
            var boolVector = ValueVector.make(DataType.BOOLEAN, 1024 * 4 /* temp */);
            var result = new ExpressionResult();
            result.vector = boolVector;
            if (isLVectorFlat && isRVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    boolVector.set(0, leftResult.vector.getBoolean(leftResult.currentIdx)
                        || rightResult.vector.getBoolean(rightResult.currentIdx));
                    result.size = 1;
                    result.currentIdx = 0;
                    return result;
                };
            } else if (!isLVectorFlat && isRVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    var rightValue = rightResult.vector.getBoolean(rightResult.currentIdx);
                    boolean val;
                    for (var i = 0; i < leftResult.size; i++) {
                        val = leftResult.vector.getBoolean(i) || rightValue;
                        boolVector.set(i, val);
                    }
                    result.size = leftResult.size;
                    return result;
                };
            } else if (isLVectorFlat) {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    var leftValue = leftResult.vector.getBoolean(leftResult.currentIdx);
                    boolean val;
                    for (var i = 0; i < rightResult.size; i++) {
                        val = leftValue || rightResult.vector.getBoolean(i);
                        boolVector.set(i, val);
                    }
                    result.size = rightResult.size;
                    return result;
                };
            } else {
                return () -> {
                    var leftResult = leftExprEval.evaluate();
                    var rightResult = rightExprEval.evaluate();
                    boolean val;
                    for (var i = 0; i < rightResult.size; i++) {
                        val = leftResult.vector.getBoolean(i) || rightResult.vector.getBoolean(i);
                        boolVector.set(i, val);
                    }
                    result.size = rightResult.size;
                    return result;
                };
            }
        }
    }
}
