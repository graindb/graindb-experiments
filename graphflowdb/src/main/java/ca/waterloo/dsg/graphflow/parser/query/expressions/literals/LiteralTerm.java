package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.DataChunks;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionEvaluator;
import ca.waterloo.dsg.graphflow.parser.query.expressions.evaluator.ExpressionResult;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import java.util.HashSet;
import java.util.Set;

public abstract class LiteralTerm extends Expression {

    protected ValueVector values;

    public LiteralTerm(String variableName, DataType dataType) {
        super(variableName, dataType);
    }

    @Override
    public String getPrintableExpression() {
        return getVariableName();
    }

    @Override
    public ExpressionEvaluator getEvaluator(DataChunks dataChunks) {
        var result = new ExpressionResult();
        result.vector = values;
        result.size = 1;
        result.currentIdx = 0;
        return () -> result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getDependentVariableNames() {
        return new HashSet<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getDependentExpressionVariableNames() {
        return new HashSet<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<PropertyVariable> getDependentPropertyVariables() {
        return new HashSet<>();
    }

    public abstract Object getLiteral();
}
