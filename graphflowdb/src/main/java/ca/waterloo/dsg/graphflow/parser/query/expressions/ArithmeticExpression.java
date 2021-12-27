package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionResult;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

public class ArithmeticExpression extends AbstractBinaryOperatorExpression {

    public enum ArithmeticOperator {
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULO("%"),
        POWER("^");

        private String symbol;
        ArithmeticOperator(String symbol) {
            this.symbol = symbol;
        }
    }

    @Getter private ArithmeticOperator arithmeticOperator;

    public ArithmeticExpression(ArithmeticOperator arithmeticOperator, Expression leftExpression,
        Expression rightExpression) {
        super(leftExpression, rightExpression);
        this.arithmeticOperator = arithmeticOperator;
        setVariableName(getPrintableExpression());
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var leftExprEval = leftExpression.getEvaluator(dataChunks);
        var rightExprEval = rightExpression.getEvaluator(dataChunks);
        var isLVectorFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
        var isRVectorFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
        var result = new ExpressionResult();
        switch (dataType) {
            case INT:
                var intVector = ValueVector.make(DataType.INT, 1024 * 4 /* temp */);
                result.vector = intVector;
                switch (arithmeticOperator) {
                    case ADD:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                intVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    + rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                for (var i = 0; i < leftResult.size; i++) {
                                    intVector.set(i, leftResult.vector.getInt(i) + rightValue);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                for (var i = 0; i < rightResult.size; i++) {
                                    intVector.set(i, leftValue + rightResult.vector.getInt(i));
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        } else {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                for (var i = 0; i < rightResult.size; i++) {
                                    intVector.set(i, leftResult.vector.getInt(i) +
                                        rightResult.vector.getInt(i));
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    default:
                        throw new UnsupportedOperationException("Arithmetic operator " +
                            arithmeticOperator.name() + " is not supported in expressions.");
                }
            default:
                throw new UnsupportedOperationException("We do not support arithmetic expressions "
                    + " on: " + dataType.name() + ".");
        }
    }

    @Override
    public String getPrintableExpression() {
        return leftExpression.getPrintableExpression() + " " + arithmeticOperator.symbol + " "
            + rightExpression.getPrintableExpression();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        checkExpressionIsNumericDataType(leftExpression);
        checkExpressionIsNumericDataType(rightExpression);
        if (DataType.DOUBLE == leftExpression.getDataType() ||
            DataType.DOUBLE == rightExpression.getDataType()) {
            setDataType(DataType.DOUBLE);
        } else  if (DataType.INT == leftExpression.getDataType() ||
            DataType.INT == rightExpression.getDataType()) {
            if (ArithmeticOperator.DIVIDE == arithmeticOperator) {
                setDataType(DataType.DOUBLE);
            } else {
                setDataType(DataType.INT);
            }
        } else { // Both data types must be ints;
            if (ArithmeticOperator.DIVIDE == arithmeticOperator) {
                setDataType(DataType.DOUBLE);
            } else {
                setDataType(DataType.INT);
            }
        }
    }

    private void checkExpressionIsNumericDataType(Expression expression) {
        if (DataType.DOUBLE != expression.getDataType() &&
            DataType.INT != expression.getDataType()) {
            throw new MalformedQueryException("An operand, i.e., left or right expression, in an " +
                "arithmethic expression is not a numeric type: " + leftExpression.getDataType());
        }
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        hash = 31*hash + arithmeticOperator.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherArithmeticExpr = (ArithmeticExpression) o;
        return this.arithmeticOperator == otherArithmeticExpr.arithmeticOperator &&
            this.leftExpression.equals(otherArithmeticExpr.leftExpression) &&
            this.rightExpression.equals(otherArithmeticExpr.rightExpression);
    }
}
