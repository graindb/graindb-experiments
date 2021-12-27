package ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.Column;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;

public abstract class NodePropertyWithOffsetReader extends PropertyReader {

    public NodePropertyWithOffsetReader(PropertyVariable propertyVariable) {
        super(propertyVariable);
    }

    @Override
    protected void allocateOutVector() {
        outVector = ValueVector.makeWithOffset(variable.getDataType());
    }

    public static NodePropertyWithOffsetReader make(PropertyVariable variable, NodePropertyStore store) {
        switch (variable.getDataType()) {
            case INT:
                return new NodePropertyIntWithOffsetReader(variable, store);
            case DOUBLE:
                return new NodePropertyDoubleWithOffsetReader(variable, store);
            default:
                throw new UnsupportedOperationException("Reading properties for data type: " +
                    variable.getDataType() + " is not yet supported in NodePropertyReader");
        }
    }

    public static class NodePropertyIntWithOffsetReader extends NodePropertyWithOffsetReader {

        ColumnInteger column;

        public NodePropertyIntWithOffsetReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnInteger) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            var nodeOffset = inVector.getNodeOffset(0);
            outVector.set(column.getPropertySlot(nodeOffset));
            outVector.setPosOffset(Column.getSlotOffset(nodeOffset));
        }
    }

    public static class NodePropertyDoubleWithOffsetReader extends NodePropertyWithOffsetReader {

        ColumnDouble column;

        public NodePropertyDoubleWithOffsetReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnDouble) store.getColumn(
                ((NodeVariable) variable.getNodeOrRelVariable()).getType(),
                variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            var nodeOffset = inVector.getNodeOffset(0);
            outVector.set(column.getPropertySlot(nodeOffset));
            outVector.setPosOffset(Column.getSlotOffset(nodeOffset));
        }
    }
}
