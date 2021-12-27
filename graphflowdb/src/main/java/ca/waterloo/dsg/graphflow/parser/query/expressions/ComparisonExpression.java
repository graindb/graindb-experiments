package ca.waterloo.dsg.graphflow.parser.query.expressions;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.MalformedQueryException;
import ca.waterloo.dsg.graphflow.parser.ParserMethodReturnValue;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionResult;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.DoubleLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.LiteralTerm;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.tuple.Schema;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import lombok.Setter;

/**
 * TODO: This class needs to be simplified with a rewrite of A+ Indexes code. Currently the A+
 *  indexes code allows WHERE clauses in the index definitions that are only conjunctive queries
 *  which consist of a list of {@link ComparisonExpression}s. This code currently requires knowing
 *  of a ComparisonType. Eventually a comparison expression can be something very complex,
 *  say containing sub-expressions and should not contain a ComparisonType as a field because
 *  the types might be quite complex. Instead if necessary it can provide
 *  isEdgePropertyAndLiteralComparison() like helper methods.
 */
public class ComparisonExpression extends AbstractBinaryOperatorExpression implements
    ParserMethodReturnValue {

    @Getter @Setter private ComparisonOperator comparisonOperator;

    public ComparisonExpression(ComparisonOperator comparisonOperator, Expression leftExpression,
        Expression rightExpression) {
        super(leftExpression, rightExpression);
        this.comparisonOperator = comparisonOperator;
        setVariableName(getPrintableExpression());
        makeRightOperandLiteralIfNecessary();
        ensureComparisonIsInclusive();
    }

    @Override
    public void verifyVariablesAndNormalize(Schema inputSchema, Schema matchGraphSchema,
        GraphCatalog catalog) {
        leftExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        rightExpression.verifyVariablesAndNormalize(inputSchema, matchGraphSchema, catalog);
        if (leftExpression.getDataType() != rightExpression.getDataType()) {
            if (!(leftExpression.getDataType().isNumeric()) ||
                !(rightExpression.getDataType().isNumeric())) {
                throw new MalformedQueryException("Type error: Left and right data types are " +
                    "inconsistent in comparison expression: " + getPrintableExpression() + " " +
                    "Left data type: " + leftExpression.getDataType() + " right data type: " +
                    rightExpression.getDataType());
            }
        }
        setDataType(DataType.BOOLEAN);
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var leftExprEval = leftExpression.getEvaluator(dataChunks);
        var rightExprEval = rightExpression.getEvaluator(dataChunks);
        var isLVectorFlat = ExpressionUtils.isExpressionOutputFlat(leftExpression, dataChunks);
        var isRVectorFlat = ExpressionUtils.isExpressionOutputFlat(rightExpression, dataChunks);
        var boolVector = ValueVector.make(DataType.BOOLEAN, 1024 * 1000 /* temp */);
        var result = new ExpressionResult();
        result.vector = boolVector;
        switch (leftExpression.getDataType()) {
            case INT:
                switch (comparisonOperator) {
                    case EQUALS:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                        == rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) == rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue == rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) == rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    case NOT_EQUALS:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    != rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) != rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue != rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) != rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    case GREATER_THAN_OR_EQUAL:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    >= rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) >= rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue >= rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) >= rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    case GREATER_THAN:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    > rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) > rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue > rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) > rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    case LESS_THAN_OR_EQUAL:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    <= rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) <= rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue <= rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) <= rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }
                    case LESS_THAN:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                boolVector.set(0, leftResult.vector.getInt(leftResult.currentIdx)
                                    < rightResult.vector.getInt(rightResult.currentIdx));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var rightValue = rightResult.vector.getInt(rightResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < leftResult.size; i++) {
                                    val = leftResult.vector.getInt(i) < rightValue;
                                    boolVector.set(i, val);
                                }
                                result.size = leftResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var leftResult = leftExprEval.evaluate();
                                var rightResult = rightExprEval.evaluate();
                                var leftValue = leftResult.vector.getInt(leftResult.currentIdx);
                                boolean val;
                                for (var i = 0; i < rightResult.size; i++) {
                                    val = leftValue < rightResult.vector.getInt(i);
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
                                    val = leftResult.vector.getInt(i) < rightResult.vector.getInt(i);
                                    boolVector.set(i, val);
                                }
                                result.size = rightResult.size;
                                return result;
                            };
                        }

                    default:
                        throw new UnsupportedOperationException("Comparison operator " +
                            comparisonOperator.name() + " is not supported for ints.");
                }
            case STRING:
                switch (comparisonOperator) {
                    case EQUALS:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var lVal = lResult.vector.getString(lResult.currentIdx);
                                boolVector.set(0, lVal != null && lVal.equals(
                                    rResult.vector.getString(rResult.currentIdx)));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var rVal = rResult.vector.getString(rResult.currentIdx);
                                var rValNotNull = rVal != null;
                                boolean val;
                                for (var i = 0; i < lResult.size; i++) {
                                    val = rValNotNull && rVal.equals(lResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = lResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var lVal = lResult.vector.getString(lResult.currentIdx);
                                var lValNotNull = lVal != null;
                                boolean val;
                                for (var i = 0; i < rResult.size; i++) {
                                    val = lValNotNull && lVal.equals(rResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = rResult.size;
                                return result;
                            };
                        } else {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                boolean val;
                                for (var i = 0; i < rResult.size; i++) {
                                    var lVal = lResult.vector.getString(i);
                                    val = lVal != null && lVal.equals(rResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = rResult.size;
                                return result;
                            };
                        }
                    case NOT_EQUALS:
                        if (isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var lVal = lResult.vector.getString(lResult.currentIdx);
                                boolVector.set(0,
                                    lVal != null && !lVal.equals(
                                        rResult.vector.getString(rResult.currentIdx)));
                                result.size = 1;
                                result.currentIdx = 0;
                                return result;
                            };
                        } else if (!isLVectorFlat && isRVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var rVal = rResult.vector.getString(rResult.currentIdx);
                                var rValNotNull = rVal != null;
                                boolean val;
                                for (var i = 0; i < lResult.size; i++) {
                                    val = rValNotNull && !rVal.equals(lResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = lResult.size;
                                return result;
                            };
                        } else if (isLVectorFlat) {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                var lVal = lResult.vector.getString(lResult.currentIdx);
                                var lValNotNull = lVal != null;
                                boolean val;
                                for (var i = 0; i < rResult.size; i++) {
                                    val = lValNotNull && !lVal.equals(rResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = rResult.size;
                                return result;
                            };
                        } else {
                            return () -> {
                                var lResult = leftExprEval.evaluate();
                                var rResult = rightExprEval.evaluate();
                                boolean val;
                                for (var i = 0; i < rResult.size; i++) {
                                    var lVal = lResult.vector.getString(i);
                                    val = lVal != null && !lVal.equals(rResult.vector.getString(i));
                                    boolVector.set(i, val);
                                }
                                result.size = rResult.size;
                                return result;
                            };
                        }
                    default:
                        throw new UnsupportedOperationException("Comparison operator " +
                            comparisonOperator.name() + " is not supported for Strings.");
                }
            default:
                throw new UnsupportedOperationException("We do not yet support comparing data " +
                    "type: " + leftExpression.getDataType().name() + " in comparison evaluators");
        }
    }

    public LiteralTerm getLiteralTerm() {
        if (!(rightExpression instanceof LiteralTerm)) {
            return null;
        }
        return ((LiteralTerm) rightExpression);
    }

    private void ensureComparisonIsInclusive() {
        if (!(rightExpression instanceof LiteralTerm)) {
            return;
        }
        var literalTerm = ((LiteralTerm) rightExpression);
        if (comparisonOperator == ComparisonOperator.LESS_THAN) {
            comparisonOperator = ComparisonOperator.LESS_THAN_OR_EQUAL;
            if (literalTerm instanceof IntLiteral) {
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() - 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                var doubleLiteral = (DoubleLiteral) literalTerm;
                doubleLiteral.setNewLiteralValue(doubleLiteral.getDoubleLiteral() - Double.MIN_VALUE);
            }
        } else if (comparisonOperator == ComparisonOperator.GREATER_THAN) {
            comparisonOperator = ComparisonOperator.GREATER_THAN_OR_EQUAL;
            if (literalTerm instanceof IntLiteral) {
                var longLiteral = (IntLiteral) literalTerm;
                longLiteral.setNewLiteralValue(longLiteral.getIntLiteral() + 1);
            } else if (literalTerm instanceof DoubleLiteral) {
                var doubleLiteral = (DoubleLiteral) literalTerm;
                doubleLiteral.setNewLiteralValue(doubleLiteral.getDoubleLiteral() + Double.MIN_VALUE);
            }
        }
    }

    private void makeRightOperandLiteralIfNecessary() {
        if (leftExpression instanceof LiteralTerm) {
            var tmp = rightExpression;
            rightExpression = leftExpression;
            leftExpression = tmp;
            comparisonOperator = comparisonOperator.getReverseOperator();
        }
    }

    @Override
    public String toString() {
        var stringBuilder = new StringBuilder();
        stringBuilder.append(leftExpression.getVariableName());
        stringBuilder.append(" ").append(comparisonOperator.toString()).append(" ");
        if (null != rightExpression) {
            stringBuilder.append(rightExpression.getVariableName());
        }
        return stringBuilder.toString();
    }

    @Override
    public int hashCode() {
        var hash = super.hashCode();
        switch (comparisonOperator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return constructReverse().hashCode();
        }
        hash = 31*hash + comparisonOperator.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        switch (comparisonOperator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return constructReverse().equals(o);
        }
        var other = (ComparisonExpression) o;
        return leftExpression.equals(other.leftExpression) &&
            rightExpression.equals(other.rightExpression) &&
            comparisonOperator == other.comparisonOperator;
    }

    private ComparisonExpression constructReverse() {
        return new ComparisonExpression(
            comparisonOperator.getReverseOperator(), rightExpression, leftExpression);
    }

    @Override
    public String getPrintableExpression() {
        return leftExpression.getPrintableExpression() + " " + comparisonOperator.getSymbol() + " "
            + rightExpression.getPrintableExpression();
    }
}
