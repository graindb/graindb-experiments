package ca.waterloo.dsg.graphflow.plan.operator.propertyreader.rel;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.relpropertylists.RelPropertyListInteger;

public abstract class RelPropertyReader extends PropertyReader {

    final boolean byDstType;
    boolean nodeOffsetFlat;
    protected ValueVector inVectorNode;
    protected DataChunk inVectorDataChunk;

    public RelPropertyReader(PropertyVariable variable, boolean byDstType) {
        super(variable);
        this.byDstType = byDstType;
    }

    public static RelPropertyReader make(PropertyVariable variable, RelPropertyStore store,
        GraphCatalog catalog) {
        var label = ((RelVariable) variable.getNodeOrRelVariable()).getLabel();
        var isDstType = !catalog.labelDirectionHasMultiplicityOne(label, Direction.FORWARD) &&
            catalog.labelDirectionHasMultiplicityOne(label, Direction.BACKWARD);
        switch (variable.getDataType()) {
            case INT:
                return new RelPropertyIntReader(variable, isDstType);
            default:
                throw new UnsupportedOperationException("Reading properties for data type: " +
                    variable.getDataType() + " is not yet supported in RelPropertyReader");
        }
    }

    public static class RelPropertyIntReader extends RelPropertyReader {

        RelPropertyListInteger list;

        public RelPropertyIntReader(PropertyVariable propertyVariable, boolean byDstType) {
            super(propertyVariable, byDstType);
        }

        @Override
        protected void initFurther(Graph graph) {
            super.initFurther(graph);
            var propertyKey = graph.getGraphCatalog().getRelPropertyKey(variable.getPropertyName());
            var relVariable = (RelVariable) variable.getNodeOrRelVariable();
            var nodeVariable = byDstType ? relVariable.getDstNode() : relVariable.getSrcNode();
            var nodeVarName = nodeVariable.getVariableName();
            inVectorNode = dataChunks.getValueVector(nodeVarName);
            nodeOffsetFlat = dataChunks.isFlat(nodeVarName);
            if (nodeOffsetFlat) {
                inVectorDataChunk = dataChunks.getDataChunk(nodeVarName);
            }
            list = (RelPropertyListInteger) graph.getRelPropertyStore().getPropertyList(
                relVariable.getLabel(), nodeVariable.getType(), propertyKey);
        }

        @Override
        protected void readValues() {
            it.init();
            if (nodeOffsetFlat) {
                var nodeOffset = inVectorNode.getNodeOffset(inVectorDataChunk.currentPos);
                for (var i = 0; i < dataChunk.size(); i++) {
                    if (selector[i]) {
                        outVector.set(i, list.getProperty(nodeOffset, it.getNextRelBucketOffset()));
                    }
                    it.moveCursor();
                }
            } else {
                for (var i = 0; i < dataChunk.size(); i++) {
                    if (selector[i]) {
                        outVector.set(i, list.getProperty(inVectorNode.getNodeOffset(i),
                            it.getNextRelBucketOffset()));
                    }
                    it.moveCursor();
                }
            }
        }
    }
}
