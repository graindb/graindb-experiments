package ca.waterloo.dsg.graphflow.tuple;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Schema of a tuple or table, i.e., an unordered set of (variable variableName, {@link DataType) pairs.
 */
public class Schema implements Serializable {
    private static final Logger logger = LogManager.getLogger(Schema.class);

    Map<String, Expression> varNameToExpressionMap;
    @Getter FactorizationStructure fStructure;

    public Schema() {
        varNameToExpressionMap = new HashMap<>();
        fStructure = new FactorizationStructure();
    }

    public boolean containsVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName);
    }

    public boolean containsRelVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName) &&
            (DataType.RELATIONSHIP == varNameToExpressionMap.get(varName).getDataType());
    }

    public boolean containsNodeVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName) &&
            (DataType.NODE == varNameToExpressionMap.get(varName).getDataType());
    }

    public Expression getExpression(String varName) {
        if (!varNameToExpressionMap.containsKey(varName)) {
            throw new IllegalArgumentException("Trying to fetch Expression for varName: " +
                varName + " which is not present in the schema");
        }
        return varNameToExpressionMap.get(varName);
    }

    public List<String> getVarNames() {
        return new ArrayList<>(varNameToExpressionMap.keySet());
    }

    public Set<NodeVariable> getNodeVariables() {
        return varNameToExpressionMap.values().stream().filter(
            expression -> DataType.NODE == expression.getDataType()).
            map(expression -> (NodeVariable) expression)
            .collect(Collectors.toSet());
    }

    public Set<RelVariable> getRelVariables() {
        return varNameToExpressionMap.values().stream().filter(
            expression -> DataType.RELATIONSHIP == expression.getDataType()).
            map(expression -> (RelVariable) expression)
            .collect(Collectors.toSet());
    }

    public Set<Entry<String, Expression>> getVariablesInLexOrder() {
        return varNameToExpressionMap.entrySet();
    }

    public void addRelVariable(RelVariable relVariable) {
        add(relVariable.getVariableName(), relVariable);
        addNodeVariable(relVariable.getSrcNode());
        addNodeVariable(relVariable.getDstNode());
    }

    public void addNodeVariable(NodeVariable nodeVariable) {
        add(nodeVariable.getVariableName(), nodeVariable);
    }

    public void add(String varName, Expression expression) {
        if (null == varName || null == expression) {
            logger.error("inputs to " + this.getClass().getSimpleName() +
                ".addVariable(String varName, DataType resultDataType) contain a null. varName: "
                + varName + " expression: " + expression);
            return;
        }
        if (!containsVarName(varName)) {
            varNameToExpressionMap.put(varName, expression);
        } else {
            if (!expression.equals(varNameToExpressionMap.get(varName))) {
                throw new IllegalArgumentException("Trying to insert to variables to the schema " +
                    "with two different expressions. Variable variableName: "  + varName + " " +
                    "Previous expression: " + varNameToExpressionMap.get(varName) + ". " +
                    "New type: " + expression);
            }
        }
    }

    public Schema project(Set<String> varNames) {
        var schema = new Schema();
        int[] fGroupMapping = null;
        if (!fStructure.getFGroups().isEmpty()) {
            fGroupMapping = new int[fStructure.getFGroups().size()];
            Arrays.fill(fGroupMapping, -1);
        }
        for (var varName : varNames) {
            if (!containsVarName(varName)) {
                throw new IllegalArgumentException("Cannot project onto variable: " + varName
                    + ". Schema does not contain that variable name.");
            }
            schema.add(varName, getExpression(varName));
            if (hasFGroup(varName)) {
                var originalFGroupIdx = getFGroupIdx(varName);
                var newFGroupIdx = fGroupMapping[originalFGroupIdx];
                if (-1 == fGroupMapping[originalFGroupIdx]) {
                    fGroupMapping[originalFGroupIdx] = schema.addNewFGroup(varName);
                } else {
                    schema.addToFGroup(varName, newFGroupIdx);
                }
            }
        }
        return schema;
    }

    public static Schema union(Schema schema1, Schema schema2) {
        Schema unionedSchema = new Schema();
        schema1.varNameToExpressionMap.forEach((key, value) ->
            unionedSchema.varNameToExpressionMap.put(key, value));
        schema2.varNameToExpressionMap.forEach((key, value) -> {
            if (unionedSchema.varNameToExpressionMap.containsKey(key) &&
                !unionedSchema.varNameToExpressionMap.get(key).equals(value)) {
                throw new IllegalArgumentException("Trying to union two schemas with variables " +
                    "that have inconsistent types. Schema1's data type for param: " +
                    key + " is " + unionedSchema.varNameToExpressionMap.get(key) +
                    "; the type in schema2 is: " + value);
            }
            unionedSchema.varNameToExpressionMap.put(key, value);
        });
        return unionedSchema;
    }

    public String getVariableNamesAsString() {
        var sb = new StringBuilder();
        var varNames = getVarNames();
        for (int i = 0; i < varNames.size(); ++i) {
            sb.append(i == 0 ? "" : ",");
            sb.append(varNames.get(i));
        }
        return sb.toString();
    }

    public boolean isEmpty() {
        return varNameToExpressionMap.isEmpty();
    }

    public boolean hasFGroups() {
        return !fStructure.getFGroups().isEmpty();
    }

    public Set<String> getFGroup(int idx) {
        return fStructure.fGroups.get(idx);
    }

    public int getFGroupIdx(String varName) {
        if (hasFGroup(varName)) {
            return fStructure.varNameFGroupMap.get(varName);
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    public Set<String> getFGroup(String varName) {
        if (hasFGroup(varName)) {
            return getFGroup(getFGroupIdx(varName));
        }
        return Collections.EMPTY_SET;
    }

    public boolean hasFGroup(String varName) {
        return fStructure.varNameFGroupMap.containsKey(varName);
    }

    public int addNewFGroup(String varName) {
        if (fStructure.varNameFGroupMap.containsKey(varName)) {
            throw new IllegalArgumentException("variableName: " + varName + " is already in a " +
                "factorization group with index: " + fStructure.varNameFGroupMap.get(varName) + ". Cannot " +
                "start a new factorization group that includes " + varName + ".");
        }
        fStructure.fGroups.add(new HashSet<>());
        fStructure.fGroups.get(fStructure.nextFGroupIdx).add(varName);
        fStructure.varNameFGroupMap.put(varName, fStructure.nextFGroupIdx);
        return fStructure.nextFGroupIdx++;
    }

    public void addToFGroup(String varName, int fGroupIdx) {
        if (fStructure.varNameFGroupMap.containsKey(varName)) {
            throw new IllegalArgumentException("variableName: " + varName + " is already in a " +
                "factorization group with index: " + fStructure.varNameFGroupMap.get(varName) + ". Cannot " +
                "add it to a factorization group again: " + varName + ".");
        }
        if (fGroupIdx >= fStructure.nextFGroupIdx) {
            throw new IllegalArgumentException("Cannot insert " + varName + " to factorization " +
                "group " + fGroupIdx + ". There is no factorization group with index: "
                + fGroupIdx + ".");
        }
        fStructure.varNameFGroupMap.put(varName, fGroupIdx);
        fStructure.fGroups.get(fGroupIdx).add(varName);
    }

    public void removeFromFGroup(String varName) {
        if (!hasFGroup(varName)) {
            throw new IllegalArgumentException("Trying to remove " + varName + " from an fgroup " +
                "but " + varName + " is not part of any fGroup.");
        }
        var idx = getFGroupIdx(varName);
        fStructure.fGroups.get(idx).remove(varName);
        fStructure.varNameFGroupMap.remove(varName);
    }

    public Schema copy() {
        Schema copy = new Schema();
        this.varNameToExpressionMap.forEach((key, value) -> copy.varNameToExpressionMap.put(key,
            value));
        copy.fStructure = this.fStructure.copy();
        return copy;
    }

    /**
     * isSame() just checks the varNames and corresponding dataType of the expressions in the
     * varNameToExpressionMap.
     * */
    public boolean isSame(Schema other) {
        if (this.varNameToExpressionMap.size() != other.varNameToExpressionMap.size()) {
            return false;
        }
        if (!this.varNameToExpressionMap.keySet().equals(other.varNameToExpressionMap.keySet())) {
            return false;
        }
        for (var key : varNameToExpressionMap.keySet()) {
            if (varNameToExpressionMap.get(key).getDataType() !=
                other.varNameToExpressionMap.get(key).getDataType()) {
                return false;
            }
        }
        return true;
    }

    public static class FactorizationStructure implements Serializable {

        Map<String, Integer> varNameFGroupMap;
        @Getter List<Set<String>> fGroups;
        private int nextFGroupIdx;

        public FactorizationStructure() {
            varNameFGroupMap = new HashMap<>();
            fGroups = new ArrayList<>();
            nextFGroupIdx = 0;
        }

        public FactorizationStructure copy() {
            FactorizationStructure copy =  new FactorizationStructure();
            this.varNameFGroupMap.forEach((key, value) -> copy.varNameFGroupMap.put(key, value));
            this.fGroups.forEach(fGroup -> {
                copy.fGroups.add(new HashSet<>());
                copy.fGroups.get(copy.fGroups.size()-1).addAll(fGroup);
            });
            copy.nextFGroupIdx = this.nextFGroupIdx;
            return copy;
        }
    }
}
