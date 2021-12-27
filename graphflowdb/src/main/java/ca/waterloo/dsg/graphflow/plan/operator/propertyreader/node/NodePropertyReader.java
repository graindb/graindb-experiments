package ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node;

import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnBoolean;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnDouble;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnInteger;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.column.ColumnString;

public abstract class NodePropertyReader extends PropertyReader {

    public NodePropertyReader(PropertyVariable propertyVariable) {
        super(propertyVariable);
    }

    public static NodePropertyReader make(PropertyVariable variable, NodePropertyStore store) {
        switch (variable.getDataType()) {
            case INT:
                return new NodePropertyIntReader(variable, store);
            case DOUBLE:
                return new NodePropertyDoubleReader(variable, store);
            case BOOLEAN:
                return new NodePropertyBoolReader(variable, store);
            case STRING:
                return new NodePropertyStringReader(variable, store);
            default:
                throw new UnsupportedOperationException("Reading properties for data type: " +
                    variable.getDataType() + " is not yet supported in NodePropertyReader");
        }
    }

    public static class NodePropertyIntReader extends NodePropertyReader {

        ColumnInteger column;

        public NodePropertyIntReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnInteger) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            it.init();
            for (var i = 0; i < dataChunk.size(); i++) {
                if (selector[i]) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                }
                it.moveCursor();
            }
        }
    }

    public static class NodePropertyDoubleReader extends NodePropertyReader {

        ColumnDouble column;

        public NodePropertyDoubleReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnDouble) store.getColumn(
                ((NodeVariable) variable.getNodeOrRelVariable()).getType(),
                variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            it.init();
            for (var i = 0; i < dataChunk.size(); i++) {
                if (selector[i]) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                }
                it.moveCursor();
            }
        }
    }

    public static class NodePropertyBoolReader extends NodePropertyReader {

        ColumnBoolean column;

        public NodePropertyBoolReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnBoolean) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            it.init();
            for (var i = 0; i < dataChunk.size(); i++) {
                if (selector[i]) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                }
                it.moveCursor();
            }
        }
    }

    public static class NodePropertyStringReader extends NodePropertyReader {

        ColumnString column;

        public NodePropertyStringReader(PropertyVariable variable, NodePropertyStore store) {
            super(variable);
            column = (ColumnString) store.getColumn(((NodeVariable) variable.
                getNodeOrRelVariable()).getType(), variable.getPropertyKey());
        }

        @Override
        protected void readValues() {
            it.init();
            for (var i = 0; i < dataChunk.size(); i++) {
                if (selector[i]) {
                    outVector.set(i, column.getProperty(it.getNextNodeOffset()));
                }
                it.moveCursor();
            }
        }
    }
}
