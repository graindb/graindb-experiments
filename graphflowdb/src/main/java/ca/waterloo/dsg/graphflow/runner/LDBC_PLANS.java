package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.query.expressions.BooleanConnectorExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.StringLiteral;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjListsNoSelReset;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextendwithtypefilter.FlattenAndExtendAdjListsNoSelResetWithType;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextendwithtypefilter.FlattenAndExtendAdjListsWithType;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextend.ExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextend.ExtendAdjListsNoSelReset;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextendwithtypefilter.ExtendAdjListsWithType;
import ca.waterloo.dsg.graphflow.plan.operator.extend.column.ExtendColumn;
import ca.waterloo.dsg.graphflow.plan.operator.extend.column.ExtendColumnWithType;
import ca.waterloo.dsg.graphflow.plan.operator.filter.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.filter.FilterNoAnd;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node.NodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node.NodePropertyWithOffsetReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.rel.RelPropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.rel.RelPropertyReader.RelPropertyIntReader;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanAllNodes;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanAllNodesNoSelReset;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCopy;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCount;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

// By default, this plan is for LDBC10

public class LDBC_PLANS {

    public static Operator H1_F(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(1267302820));
        var filterDate = new Filter(dateGreaterThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator H1_B(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(1267302820));
        var filterDate = new Filter(dateGreaterThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator H2_F(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar2 = makeRelVar(graph.getGraphCatalog(),
                "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        sink.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(sink);

        var readerDate2 = RelPropertyIntReader.make(cDateVar2, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filter_k1_less_k2.setPrev(readerDate2);
        readerDate2.setNext(filter_k1_less_k2);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        readerDate2.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerDate2);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        flattenAndExtendToP3.setPrev(readerDate);
        readerDate.setNext(flattenAndExtendToP3);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodesNoSelReset(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator H2_B(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar2 = makeRelVar(graph.getGraphCatalog(),
                "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        sink.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(sink);

        var readerDate2 = RelPropertyIntReader.make(cDateVar2, graph.getRelPropertyStore(), catalog);
        filter_k1_less_k2.setPrev(readerDate2);
        readerDate2.setNext(filter_k1_less_k2);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD));
        readerDate2.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerDate2);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(), catalog);
        flattenAndExtendToP3.setPrev(readerDate);
        readerDate.setNext(flattenAndExtendToP3);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodesNoSelReset(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator H3_F(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar3 = makeRelVar(graph.getGraphCatalog(),
                "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD);
        var cDateVar3 = new PropertyVariable(relVar3, "date", DataType.INT);

        var relVar2 = makeRelVar(graph.getGraphCatalog(),
                "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cmp_k2_less_k3 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar2, cDateVar3);
        var filter_k2_less_k3 = new Filter(cmp_k2_less_k3);
        sink.setPrev(filter_k2_less_k3);
        filter_k2_less_k3.setNext(sink);

        var readerDate3 = RelPropertyIntReader.make(cDateVar3, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filter_k2_less_k3.setPrev(readerDate3);
        readerDate3.setNext(filter_k2_less_k3);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD));
        readerDate3.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(readerDate3);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        flattenAndExtendToP4.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(flattenAndExtendToP4);

        var readerDate2 = RelPropertyIntReader.make(cDateVar2, graph.getRelPropertyStore(), catalog);
        filter_k1_less_k2.setPrev(readerDate2);
        readerDate2.setNext(filter_k1_less_k2);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        readerDate2.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerDate2);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(), catalog);
        flattenAndExtendToP3.setPrev(readerDate);
        readerDate.setNext(flattenAndExtendToP3);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodesNoSelReset(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator H3_B(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCount();

        var relVar3 = makeRelVar(graph.getGraphCatalog(),
                "p3", "Person", "p4", "Person", "k3", "knows", Direction.BACKWARD);
        var cDateVar3 = new PropertyVariable(relVar3, "date", DataType.INT);

        var relVar2 = makeRelVar(graph.getGraphCatalog(),
                "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cmp_k2_less_k3 = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar2, cDateVar3);
        var filter_k2_less_k3 = new Filter(cmp_k2_less_k3);
        sink.setPrev(filter_k2_less_k3);
        filter_k2_less_k3.setNext(sink);

        var readerDate3 = RelPropertyIntReader.make(cDateVar3, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filter_k2_less_k3.setPrev(readerDate3);
        readerDate3.setNext(filter_k2_less_k3);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p3", "Person", "p4", "Person", "k3", "knows", Direction.BACKWARD));
        readerDate3.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(readerDate3);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        flattenAndExtendToP4.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(flattenAndExtendToP4);

        var readerDate2 = RelPropertyIntReader.make(cDateVar2, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filter_k1_less_k2.setPrev(readerDate2);
        readerDate2.setNext(filter_k1_less_k2);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD));
        readerDate2.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerDate2);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        flattenAndExtendToP3.setPrev(readerDate);
        readerDate.setNext(flattenAndExtendToP3);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var scanP1 = new ScanAllNodesNoSelReset(new NodeVariable("p1", catalog.getTypeKey("Person")));
        flattenAndExtendToP2.setPrev(scanP1);
        scanP1.setNext(flattenAndExtendToP2);

        return sink;
    }

    public static Operator MICRO01(Graph graph, int personid) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{"p1.cid", "p1.gender", "p1.creationDate", "p1.browserUsed"}, // ,/*int Vectors Flat */,
                new String[]{"c.cid", "c.creationDate", "c.browserUsed", "c.length"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var readerCBrowser = makeNodePropReader(graph, "c", "comment", DataType.INT, "browserUsed");
        sink.setPrev(readerCBrowser);
        readerCBrowser.setNext(sink);

        var readerCLength = makeNodePropReader(graph, "c", "comment", DataType.INT, "length");
        readerCBrowser.setPrev(readerCLength);
        readerCLength.setNext(readerCBrowser);

        var readerCreationDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        readerCLength.setPrev(readerCreationDate);
        readerCreationDate.setNext(readerCLength);

        var readerCCid = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        readerCreationDate.setPrev(readerCCid);
        readerCCid.setNext(readerCreationDate);

        var readerPBrowser = makeNodePropReader(graph, "p1", "person", DataType.INT, "browserUsed");
        readerCCid.setPrev(readerPBrowser);
        readerPBrowser.setNext(readerCCid);

        var readerPCreationDate = makeNodePropReader(graph, "p1", "person", DataType.INT, "creationDate");
        readerPBrowser.setPrev(readerPCreationDate);
        readerPCreationDate.setNext(readerPBrowser);

        var readerPGender = makeNodePropReader(graph, "p1", "person", DataType.INT, "gender");
        readerPCreationDate.setPrev(readerPGender);
        readerPGender.setNext(readerPCreationDate);

        var readerPCid = makeNodePropReader(graph, "p1", "person", DataType.INT, "cid");
        readerPGender.setPrev(readerPCid);
        readerPCid.setNext(readerPGender);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelResetWithType(
                makeALD(catalog, "p1", "person", "c", "comment", "hc", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerPCid.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCid);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(personid));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO02(Graph graph, int personid) {
        var catalog = graph.getGraphCatalog();
        var forumKey = catalog.getTypeKey("forum");

        var sink = new SinkCopy(
                new String[]{"f.cid", "f.creationDate"}, // ,/*int Vectors Flat */,
                new String[]{"p.cid", "p.creationDate", "p.browserUsed", "p.length"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var readerPLength = makeNodePropReader(graph, "p", "post", DataType.INT, "length");
        sink.setPrev(readerPLength);
        readerPLength.setNext(sink);

        var readerPBrowser = makeNodePropReader(graph, "p", "post", DataType.INT, "browserUsed");
        readerPLength.setPrev(readerPBrowser);
        readerPBrowser.setNext(readerPLength);

        var readerPCreationDate = makeNodePropReader(graph, "p", "post", DataType.INT, "creationDate");
        readerPBrowser.setPrev(readerPCreationDate);
        readerPCreationDate.setNext(readerPBrowser);

        var readerPID = makeNodePropReader(graph, "p", "post", DataType.INT, "cid");
        readerPCreationDate.setPrev(readerPID);
        readerPID.setNext(readerPCreationDate);

        var readerFCreationDate = makeNodePropReader(graph, "f", "forum", DataType.INT, "creationDate");
        readerPID.setPrev(readerFCreationDate);
        readerFCreationDate.setNext(readerPID);

        var readerFID = makeNodePropReader(graph, "f", "forum", DataType.INT, "cid");
        readerFCreationDate.setPrev(readerFID);
        readerFID.setNext(readerFCreationDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "f", "forum", "p", "post", "co", "containerOf", Direction.FORWARD));
        readerFID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerFID);

        var cidVar = new PropertyVariable(new NodeVariable("f", forumKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(personid));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "f", "forum", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("f", forumKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO03(Graph graph, int personid) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p2.cid", "p2.gender", "p2.creationDate"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        // friend.id, friend.fName, friend.lName, e.creationDate

        var readerP2LName = makeNodePropReader(graph, "p2", "person", DataType.INT, "creationDate");
        sink.setPrev(readerP2LName);
        readerP2LName.setNext(sink);

        var readerP2FName = makeNodePropReader(graph, "p2", "person", DataType.INT, "gender");
        readerP2LName.setPrev(readerP2FName);
        readerP2FName.setNext(readerP2LName);

        var readerP2ID = makeNodePropReader(graph, "p2", "person", DataType.INT, "cid");
        readerP2FName.setPrev(readerP2ID);
        readerP2ID.setNext(readerP2FName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerP2ID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerP2ID);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(personid));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO04(Graph graph, int forumid) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{"p.cid", "p.gender", "p.creationDate", "p.browserUsed"}, // ,/*int Vectors Flat */,
                new String[]{"c.cid", "c.creationDate", "c.browserUsed", "c.length"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var readerCBrowser = makeNodePropReader(graph, "c", "comment", DataType.INT, "browserUsed");
        sink.setPrev(readerCBrowser);
        readerCBrowser.setNext(sink);

        var readerCLength = makeNodePropReader(graph, "c", "comment", DataType.INT, "length");
        readerCBrowser.setPrev(readerCLength);
        readerCLength.setNext(readerCBrowser);

        var readerCreationDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        readerCLength.setPrev(readerCreationDate);
        readerCreationDate.setNext(readerCLength);

        var readerCCid = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        readerCreationDate.setPrev(readerCCid);
        readerCCid.setNext(readerCreationDate);

        var readerPBrowser = makeNodePropReader(graph, "p", "person", DataType.INT, "browserUsed");
        readerCCid.setPrev(readerPBrowser);
        readerPBrowser.setNext(readerCCid);

        var readerPCreationDate = makeNodePropReader(graph, "p", "person", DataType.INT, "creationDate");
        readerPBrowser.setPrev(readerPCreationDate);
        readerPCreationDate.setNext(readerPBrowser);

        var readerPGender = makeNodePropReader(graph, "p", "person", DataType.INT, "gender");
        readerPCreationDate.setPrev(readerPGender);
        readerPGender.setNext(readerPCreationDate);

        var readerPCid = makeNodePropReader(graph, "p", "person", DataType.INT, "cid");
        readerPGender.setPrev(readerPCid);
        readerPCid.setNext(readerPGender);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelResetWithType(
                makeALD(catalog, "p", "person", "c", "comment", "l", "likes", Direction.FORWARD),
                catalog.getTypeKey("comment"));
        readerPCid.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCid);

        var cidVar = new PropertyVariable(new NodeVariable("p", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(forumid));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO05(Graph graph, int forumid) {
        var catalog = graph.getGraphCatalog();
        var forumKey = catalog.getTypeKey("forum");

        var sink = new SinkCopy(
                new String[]{"f.creationDate"}, // ,/*int Vectors Flat */,
                new String[]{"p.length"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var readerPLength = makeNodePropReader(graph, "p", "post", DataType.INT, "length");
        sink.setPrev(readerPLength);
        readerPLength.setNext(sink);

        var readerPBrowser = makeNodePropReader(graph, "p", "post", DataType.INT, "browserUsed");
        readerPLength.setPrev(readerPBrowser);
        readerPBrowser.setNext(readerPLength);

        var readerPCreationDate = makeNodePropReader(graph, "p", "post", DataType.INT, "creationDate");
        readerPBrowser.setPrev(readerPCreationDate);
        readerPCreationDate.setNext(readerPBrowser);

        var readerPID = makeNodePropReader(graph, "p", "post", DataType.INT, "cid");
        readerPCreationDate.setPrev(readerPID);
        readerPID.setNext(readerPCreationDate);

        var readerFCreationDate = makeNodePropReader(graph, "f", "forum", DataType.INT, "creationDate");
        readerPID.setPrev(readerFCreationDate);
        readerFCreationDate.setNext(readerPID);

        var readerFID = makeNodePropReader(graph, "f", "forum", DataType.INT, "cid");
        readerFCreationDate.setPrev(readerFID);
        readerFID.setNext(readerFCreationDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "f", "forum", "p", "post", "co", "containerOf", Direction.FORWARD));
        readerFID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerFID);

        var cidVar = new PropertyVariable(new NodeVariable("f", forumKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(forumid));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "f", "forum", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("f", forumKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO08(Graph graph, int personid) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCount();

        var k1RelVar = makeRelVar(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD);
        var k1DateVar = new PropertyVariable(k1RelVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN_OR_EQUAL, k1DateVar, new IntLiteral(0));
        var filterDate = new Filter(dateGreaterThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerK1CreationDate = RelPropertyReader.make(k1DateVar, graph.getRelPropertyStore(), catalog);
        filterDate.setPrev(readerK1CreationDate);
        readerK1CreationDate.setNext(filterDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerK1CreationDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerK1CreationDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarLessEqualTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(personid));
        var cidFilter = new Filter(cidVarLessEqualTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator MICRO09(Graph graph, int date) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCount();

        var k1RelVar = makeRelVar(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD);
        var k1DateVar = new PropertyVariable(k1RelVar, "date", DataType.INT);
        var dateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, k1DateVar, new IntLiteral(date));
        var filterDate = new Filter(dateLessThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerK1CreationDate = RelPropertyReader.make(k1DateVar, graph.getRelPropertyStore(), catalog);
        filterDate.setPrev(readerK1CreationDate);
        readerK1CreationDate.setNext(filterDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerK1CreationDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerK1CreationDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.GREATER_THAN_OR_EQUAL, cidVar,
                new IntLiteral(0));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }



    /**
     * (p:person)-[l:isLocatedIn]->(pl:place)
     * WHERE p1 = 22468883 (0 in our case)
     */
    public static Operator IS01(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p.browserUsed", "p.gender", "p.creationDate", "pl.cid"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"p.fName", "p.lName", "p.ip", "p.birthday"}  /* str Vectors Lists */);

        var readerPlaceID = makeNodePropReader(graph, "pl", "place", DataType.INT, "cid");
        sink.setPrev(readerPlaceID);
        readerPlaceID.setNext(sink);

        var extendToPl = new ExtendColumn(makeALD(catalog,
                "p", "person", "pl", "place", "l", "isLocatedIn", Direction.FORWARD));
        readerPlaceID.setPrev(extendToPl);
        extendToPl.setNext(readerPlaceID);

        var readerBCreationDate = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "creationDate");
        extendToPl.setPrev(readerBCreationDate);
        readerBCreationDate.setNext(extendToPl);

        var readerGender = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "gender");
        readerBCreationDate.setPrev(readerGender);
        readerGender.setNext(readerBCreationDate);

        var readerBrowserUsed = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "browserUsed");
        readerGender.setPrev(readerBrowserUsed);
        readerBrowserUsed.setNext(readerGender);

        var readerLocationIP = makeNodePropReader(graph, "p", "person", DataType.STRING, "ip");
        readerBrowserUsed.setPrev(readerLocationIP);
        readerLocationIP.setNext(readerBrowserUsed);

        var readerBYear = makeNodePropReader(graph, "p", "person", DataType.STRING, "birthday");
        readerLocationIP.setPrev(readerBYear);
        readerBYear.setNext(readerLocationIP);

        var readerLName = makeNodePropReader(graph, "p", "person", DataType.STRING, "lName");
        readerBYear.setPrev(readerLName);
        readerLName.setNext(readerBYear);

        var readerFName = makeNodePropReader(graph, "p", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var cidVar = new PropertyVariable(new NodeVariable("p", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        readerFName.setPrev(cidFilter);
        cidFilter.setNext(readerFName);

        var reader = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p", catalog.getTypeKey("person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * (p:Person)<-[:hasCreator]-(c:comment)-[:replyOf]->(post:Post)-[:hasCreator]->(op:Person)
     * WHERE p1 = 22468883 (0 in our case)
     */
    public static Operator IS02(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"c.cid", "c.creationDate", "op.cid"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"c.content", "op.fName", "op.lName"}  /* str Vectors Lists */);

        var readerOPLName = makeNodePropReader(graph, "op", "person", DataType.STRING, "lName");
        sink.setPrev(readerOPLName);
        readerOPLName.setNext(sink);

        var readerOPFName = makeNodePropReader(graph, "op", "person", DataType.STRING, "fName");
        readerOPLName.setPrev(readerOPFName);
        readerOPFName.setNext(readerOPLName);

        var readerOPID = makeNodePropReader(graph, "op", "person", DataType.INT, "cid");
        readerOPFName.setPrev(readerOPID);
        readerOPID.setNext(readerOPFName);

        var extendToOp = new ExtendColumn(makeALD(catalog,
                "pst", "post", "op", "person", "k3", "hasCreator", Direction.FORWARD));
        readerOPID.setPrev(extendToOp);
        extendToOp.setNext(readerOPID);

        var extendToPst = new ExtendColumnWithType(makeALD(catalog,
                "c", "comment", "pst", "post", "k2", "replyOf", Direction.FORWARD),
                catalog.getTypeKey("post"));
        extendToOp.setPrev(extendToPst);
        extendToPst.setNext(extendToOp);

        var readerCCreationDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        extendToPst.setPrev(readerCCreationDate);
        readerCCreationDate.setNext(extendToPst);

        var readerCContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        readerCCreationDate.setPrev(readerCContent);
        readerCContent.setNext(readerCCreationDate);

        var readerCID = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        readerCContent.setPrev(readerCID);
        readerCID.setNext(readerCContent);

        var flattenAndExtendToC = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p", "person", "c", "comment", "k1", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerCID.setPrev(flattenAndExtendToC);
        flattenAndExtendToC.setNext(readerCID);

        var cidVar = new PropertyVariable(new NodeVariable("p", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToC.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToC);

        var reader = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP = new ScanAllNodes(new NodeVariable("p", personKey));
        reader.setPrev(scanP);
        scanP.setNext(reader);

        return sink;
    }


    /**
     * (p1:person)-[l:knows]->(p2:person)
     * WHERE p1 = 22468883 (0 in our case)
     */
    public static Operator IS03(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p2.cid", "k1.date"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"p2.fName", "p2.lName", "p2.birthday"}  /* str Vectors Lists */);

        // friend.id, friend.fName, friend.lName, e.creationDate

        var k1RelVar = makeRelVar(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD);
        var k1DateVar = new PropertyVariable(k1RelVar, "date", DataType.INT);
        var readerK1CreationDate = RelPropertyReader.make(k1DateVar, graph.getRelPropertyStore(), catalog);
        sink.setPrev(readerK1CreationDate);
        readerK1CreationDate.setNext(sink);

        var readerPersonBirthday = makeNodePropReader(graph, "p2", "person", DataType.STRING, "birthday");
        readerK1CreationDate.setPrev(readerPersonBirthday);
        readerPersonBirthday.setNext(readerK1CreationDate);

        var readerP2LName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "lName");
        readerPersonBirthday.setPrev(readerP2LName);
        readerP2LName.setNext(readerPersonBirthday);

        var readerP2FName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "fName");
        readerP2LName.setPrev(readerP2FName);
        readerP2FName.setNext(readerP2LName);

        var readerP2ID = makeNodePropReader(graph, "p2", "person", DataType.INT, "cid");
        readerP2FName.setPrev(readerP2ID);
        readerP2ID.setNext(readerP2FName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerP2ID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerP2ID);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN_OR_EQUAL, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * (c:comment)
     * WHERE c = 0
     */
    public static Operator IS04(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var commentKey = catalog.getTypeKey("comment");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"c.creationDate"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"c.content"}  /* str Vectors Lists */);

        var readerContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        sink.setPrev(readerContent);
        readerContent.setNext(sink);

        var readerCreationDate = makeNodePropReaderWithOffset(graph, "c", "comment", DataType.INT, "creationDate");
        readerContent.setPrev(readerCreationDate);
        readerCreationDate.setNext(readerContent);

        var cidVar = new PropertyVariable(new NodeVariable("c", commentKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar, new IntLiteral(0));
        var cidFilter = new Filter(cidVarEqualsTo);
        readerCreationDate.setPrev(cidFilter);
        cidFilter.setNext(readerCreationDate);

        var reader = makeNodePropReaderWithOffset(graph, "c", "comment", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanC = new ScanAllNodes(new NodeVariable("c", catalog.getTypeKey("comment")));
        reader.setPrev(scanC);
        scanC.setNext(reader);

        return sink;
    }

    /**
     * (c:comment)-[l:hasCreator]->(p:person)
     * WHERE c = 0
     */
    public static Operator IS05(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var commentKey = catalog.getTypeKey("comment");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p.cid"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"p.fName", "p.lName"}  /* str Vectors Lists */);

        var readerPLName = makeNodePropReader(graph, "p", "person", DataType.STRING, "lName");
        sink.setPrev(readerPLName);
        readerPLName.setNext(sink);

        var readerPFName = makeNodePropReader(graph, "p", "person", DataType.STRING, "fName");
        readerPLName.setPrev(readerPFName);
        readerPFName.setNext(readerPLName);

        var readerPID = makeNodePropReader(graph, "p", "person", DataType.INT, "cid");
        readerPFName.setPrev(readerPID);
        readerPID.setNext(readerPFName);

        var flattenAndExtendToP = new ExtendColumn(
                makeALD(catalog, "c", "comment", "p", "person", "k1", "hasCreator", Direction.FORWARD));
        readerPID.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(readerPID);

        var cidVar = new PropertyVariable(new NodeVariable("c", commentKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(0));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP);

        var reader = makeNodePropReaderWithOffset(graph, "c", "comment", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanC = new ScanAllNodes(new NodeVariable("c", catalog.getTypeKey("comment")));
        reader.setPrev(scanC);
        scanC.setNext(reader);

        return sink;
    }

    /**
     * (c:comment)-[:replyOf]->(pst:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(p:person)
     * WHERE c = 0
     */
    public static Operator IS06(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var commentKey = catalog.getTypeKey("comment");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"f.cid", "p.cid"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"p.fName", "p.lName", "f.title"}  /* str Vectors Lists */);

        var readerFTitle = makeNodePropReader(graph, "f", "forum", DataType.STRING, "title");
        sink.setPrev(readerFTitle);
        readerFTitle.setNext(sink);

        var readerFCID = makeNodePropReader(graph, "f", "forum", DataType.INT, "cid");
        readerFTitle.setPrev(readerFCID);
        readerFCID.setNext(readerFTitle);

        var readerLName = makeNodePropReader(graph, "p", "person", DataType.STRING, "lName");
        readerFCID.setPrev(readerLName);
        readerLName.setNext(readerFCID);

        var readerFName = makeNodePropReader(graph, "p", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var readerPId = makeNodePropReader(graph, "p", "person", DataType.INT, "cid");
        readerFName.setPrev(readerPId);
        readerPId.setNext(readerFName);

        var flattenAndExtendToP = new ExtendColumn(makeALD(catalog,
                "f", "forum", "p", "person", "k3", "hasModerator", Direction.FORWARD));
        readerPId.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(readerPId);

        var flattenAndExtendToF = new ExtendColumn(makeALD(catalog,
                "pst", "post", "f", "forum", "k2", "containerOf", Direction.BACKWARD));
        flattenAndExtendToP.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(flattenAndExtendToP);

        var flattenAndExtendToPst = new ExtendColumnWithType(makeALD(catalog,
                "c", "comment", "pst", "post", "k1", "replyOf", Direction.FORWARD),
                catalog.getTypeKey("post"));
        flattenAndExtendToF.setPrev(flattenAndExtendToPst);
        flattenAndExtendToPst.setNext(flattenAndExtendToF);

        var cidVar = new PropertyVariable(new NodeVariable("c", commentKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(0));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToPst.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToPst);

        var reader = makeNodePropReaderWithOffset(graph, "c", "comment", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanC = new ScanAllNodes(new NodeVariable("c", catalog.getTypeKey("comment")));
        reader.setPrev(scanC);
        scanC.setNext(reader);

        return sink;
    }

    /**
     * (msgP:person)<-[:hasCreator]-(c0:comment)<-[:replyOf]-(c1:comment)-[:hasCreator]->(rplP:person)
     * WHERE cmt0.id = 6
     */
    public static Operator IS07(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var commentKey = catalog.getTypeKey("comment");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"rplP.cid", "c1.cid", "c1.creationDate"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"rplP.fName", "rplP.lName", "c1.content"}  /* str Vectors Lists */);

        var readerC1Content = makeNodePropReader(graph, "c1", "comment", DataType.STRING, "content");
        sink.setPrev(readerC1Content);
        readerC1Content.setNext(sink);

        var readerC1Date = makeNodePropReader(graph, "c1", "comment", DataType.INT, "creationDate");
        readerC1Content.setPrev(readerC1Date);
        readerC1Date.setNext(readerC1Content);

        var readerC1CID = makeNodePropReader(graph, "c1", "comment", DataType.INT, "cid");
        readerC1Date.setPrev(readerC1CID);
        readerC1CID.setNext(readerC1Date);

        var readerLName = makeNodePropReader(graph, "rplP", "person", DataType.STRING, "lName");
        readerC1CID.setPrev(readerLName);
        readerLName.setNext(readerC1CID);

        var readerFName = makeNodePropReader(graph, "rplP", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var readerPId = makeNodePropReader(graph, "rplP", "person", DataType.INT, "cid");
        readerFName.setPrev(readerPId);
        readerPId.setNext(readerFName);

        var flattenAndExtendToRplP = new ExtendColumn(makeALD(catalog,
                "c1", "comment", "rplP", "person", "k3", "hasCreator", Direction.FORWARD));
        readerPId.setPrev(flattenAndExtendToRplP);
        flattenAndExtendToRplP.setNext(readerPId);

        var flattenAndExtendToC1 = new FlattenAndExtendAdjLists(makeALD(catalog,
                "c0", "comment", "c1", "comment", "k2", "replyOf", Direction.BACKWARD));
        flattenAndExtendToRplP.setPrev(flattenAndExtendToC1);
        flattenAndExtendToC1.setNext(flattenAndExtendToRplP);

        var flattenAndExtendToMsgP = new ExtendColumn(makeALD(catalog,
                "c0", "comment", "msgP", "person", "k1", "hasCreator", Direction.FORWARD));
        flattenAndExtendToC1.setPrev(flattenAndExtendToMsgP);
        flattenAndExtendToMsgP.setNext(flattenAndExtendToC1);

        var cidVar = new PropertyVariable(new NodeVariable("c0", commentKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar, new IntLiteral(6));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToMsgP.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToMsgP);

        var reader = makeNodePropReaderWithOffset(graph, "c0", "comment", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanC0 = new ScanAllNodes(new NodeVariable("c0", commentKey));
        reader.setPrev(scanC0);
        scanC0.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[k1:knows]->(p2:person),
     * (p2:person)-[k2:knows]->(p3:person),
     * (p3:person)-[k3:knows]->(p4:person),
     * (p4:person)-[l4:isLocatedIn]->(pl:place)
     * WHERE p1 = 22468883 AND p4.fName = 'Rahul'
     * RETURN p4.id, p4.lName, p4.birthday, p4.creationDate, p4.gender, p4.locationIP, pl.name
     */
    public static Operator IC01(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p4.cid", "p4.creationDate", "p4.gender"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"pl.name", "p4.lName", "p4.ip"}  /* str Vectors Lists */);

        var readerPlaceID = makeNodePropReader(graph, "pl", "place", DataType.STRING, "name");
        sink.setPrev(readerPlaceID);
        readerPlaceID.setNext(sink);

        var readerPersonIP = makeNodePropReader(graph, "p4", "person", DataType.STRING, "ip");
        readerPlaceID.setPrev(readerPersonIP);
        readerPersonIP.setNext(readerPlaceID);

        var readerPersonG = makeNodePropReader(graph, "p4", "person", DataType.INT, "gender");
        readerPersonIP.setPrev(readerPersonG);
        readerPersonG.setNext(readerPersonIP);

        var readerPersonD = makeNodePropReader(graph, "p4", "person", DataType.INT, "creationDate");
        readerPersonG.setPrev(readerPersonD);
        readerPersonD.setNext(readerPersonG);

        var readerPersonLN = makeNodePropReader(graph, "p4", "person", DataType.STRING, "lName");
        readerPersonD.setPrev(readerPersonLN);
        readerPersonLN.setNext(readerPersonD);

        var readerPersonID = makeNodePropReader(graph, "p4", "person", DataType.INT, "cid");
        readerPersonLN.setPrev(readerPersonID);
        readerPersonID.setNext(readerPersonLN);

        var extendToPl = new ExtendColumn(makeALD(catalog, "p4", "person", "pl", "organisation",
                "l4", "isLocatedIn", Direction.FORWARD));
        readerPersonID.setPrev(extendToPl);
        extendToPl.setNext(readerPersonID);

        var fNameVar = new PropertyVariable(new NodeVariable("p4", personKey), "fName", DataType.STRING);
        var fNameEqualToJohn = new ComparisonExpression(ComparisonOperator.EQUALS, fNameVar,
                new StringLiteral("Rahul"));
        var filterFNameEqualToRahul = new FilterNoAnd(fNameEqualToJohn);
        extendToPl.setPrev(filterFNameEqualToRahul);
        filterFNameEqualToRahul.setNext(extendToPl);

        var readerP4FName = makeNodePropReader(graph, "p4", "person", DataType.STRING, "fName");
        filterFNameEqualToRahul.setPrev(readerP4FName);
        readerP4FName.setNext(filterFNameEqualToRahul);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p3", "person", "p4", "person", "k3", "knows", Direction.FORWARD));
        readerP4FName.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(readerP4FName);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToP4.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToP4);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[k1:knows]->(p4:person),
     * (p4:person)-[l4:isLocatedIn]->(pl:place)
     * WHERE p1 = 22468883 AND p4.fName = 'Rahul'
     * RETURN p4.id, p4.lName, p4.birthday, p4.creationDate, p4.gender, p4.locationIP, pl.name
     */
    public static Operator IC01A(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p4.cid", "p4.creationDate", "p4.gender"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"pl.name", "p4.lName", "p4.ip"}  /* str Vectors Lists */);

        var readerPlaceID = makeNodePropReader(graph, "pl", "place", DataType.STRING, "name");
        sink.setPrev(readerPlaceID);
        readerPlaceID.setNext(sink);

        var readerPersonIP = makeNodePropReader(graph, "p4", "person", DataType.STRING, "ip");
        readerPlaceID.setPrev(readerPersonIP);
        readerPersonIP.setNext(readerPlaceID);

        var readerPersonG = makeNodePropReader(graph, "p4", "person", DataType.INT, "gender");
        readerPersonIP.setPrev(readerPersonG);
        readerPersonG.setNext(readerPersonIP);

        var readerPersonD = makeNodePropReader(graph, "p4", "person", DataType.INT, "creationDate");
        readerPersonG.setPrev(readerPersonD);
        readerPersonD.setNext(readerPersonG);

        var readerPersonLN = makeNodePropReader(graph, "p4", "person", DataType.STRING, "lName");
        readerPersonD.setPrev(readerPersonLN);
        readerPersonLN.setNext(readerPersonD);

        var readerPersonID = makeNodePropReader(graph, "p4", "person", DataType.INT, "cid");
        readerPersonLN.setPrev(readerPersonID);
        readerPersonID.setNext(readerPersonLN);

        var extendToPl = new ExtendColumn(makeALD(catalog, "p4", "person", "pl", "organisation",
                "l4", "isLocatedIn", Direction.FORWARD));
        readerPersonID.setPrev(extendToPl);
        extendToPl.setNext(readerPersonID);

        var fNameVar = new PropertyVariable(new NodeVariable("p4", personKey), "fName", DataType.STRING);
        var fNameEqualToJohn = new ComparisonExpression(ComparisonOperator.EQUALS, fNameVar,
                new StringLiteral("Rahul"));
        var filterFNameEqualToRahul = new FilterNoAnd(fNameEqualToJohn);
        extendToPl.setPrev(filterFNameEqualToRahul);
        filterFNameEqualToRahul.setNext(extendToPl);

        var readerP4FName = makeNodePropReader(graph, "p4", "person", DataType.STRING, "fName");
        filterFNameEqualToRahul.setPrev(readerP4FName);
        readerP4FName.setNext(filterFNameEqualToRahul);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p4", "person", "k1", "knows", Direction.FORWARD));
        readerP4FName.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerP4FName);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[k1:knows]->(p2:person),
     * (p2:person)-[k3:knows]->(p4:person),
     * (p4:person)-[l4:isLocatedIn]->(pl:place)
     * WHERE p1 = 22468883 AND p4.fName = 'Rahul'
     * RETURN p4.id, p4.lName, p4.birthday, p4.creationDate, p4.gender, p4.locationIP, pl.name
     */
    public static Operator IC01B(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p4.cid", "p4.creationDate", "p4.gender"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"pl.name", "p4.lName", "p4.ip"}  /* str Vectors Lists */);

        var readerPlaceID = makeNodePropReader(graph, "pl", "place", DataType.STRING, "name");
        sink.setPrev(readerPlaceID);
        readerPlaceID.setNext(sink);

        var readerPersonIP = makeNodePropReader(graph, "p4", "person", DataType.STRING, "ip");
        readerPlaceID.setPrev(readerPersonIP);
        readerPersonIP.setNext(readerPlaceID);

        var readerPersonG = makeNodePropReader(graph, "p4", "person", DataType.INT, "gender");
        readerPersonIP.setPrev(readerPersonG);
        readerPersonG.setNext(readerPersonIP);

        var readerPersonD = makeNodePropReader(graph, "p4", "person", DataType.INT, "creationDate");
        readerPersonG.setPrev(readerPersonD);
        readerPersonD.setNext(readerPersonG);

        var readerPersonLN = makeNodePropReader(graph, "p4", "person", DataType.STRING, "lName");
        readerPersonD.setPrev(readerPersonLN);
        readerPersonLN.setNext(readerPersonD);

        var readerPersonID = makeNodePropReader(graph, "p4", "person", DataType.INT, "cid");
        readerPersonLN.setPrev(readerPersonID);
        readerPersonID.setNext(readerPersonLN);

        var extendToPl = new ExtendColumn(makeALD(catalog, "p4", "person", "pl", "organisation",
                "l4", "isLocatedIn", Direction.FORWARD));
        readerPersonID.setPrev(extendToPl);
        extendToPl.setNext(readerPersonID);

        var fNameVar = new PropertyVariable(new NodeVariable("p4", personKey), "fName", DataType.STRING);
        var fNameEqualToJohn = new ComparisonExpression(ComparisonOperator.EQUALS, fNameVar,
                new StringLiteral("Rahul"));
        var filterFNameEqualToRahul = new FilterNoAnd(fNameEqualToJohn);
        extendToPl.setPrev(filterFNameEqualToRahul);
        filterFNameEqualToRahul.setNext(extendToPl);

        var readerP4FName = makeNodePropReader(graph, "p4", "person", DataType.STRING, "fName");
        filterFNameEqualToRahul.setPrev(readerP4FName);
        readerP4FName.setNext(filterFNameEqualToRahul);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(
                makeALD(catalog, "p2", "person", "p4", "person", "k3", "knows", Direction.FORWARD));
        readerP4FName.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(readerP4FName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP4.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP4);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[k1:knows]->(p2:person),
     * (c:comment)-[hC:hasCreator]->(p2:person)
     * WHERE p1 = 22468883 AND c.creationDate < 1342805711
     * RETURN  p2.id, p2.fName, p2.lName, c.id, c.content, c.creationDate
     */
    public static Operator IC02(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");
        var commentKey = catalog.getTypeKey("comment");

        var sink = new SinkCopy(
                new String[]{"p2.cid"}, // ,/*int Vectors Flat */,
                new String[]{"c.cid", "c.creationDate"}
                /* int Vectors Lists */,
                new String[]{"p2.fName", "p2.lName"}, /* str Vectors Flat */
                new String[]{"c.content"}  /* str Vectors Lists */);

        var readerContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        sink.setPrev(readerContent);
        readerContent.setNext(sink);

        var readerCCID = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        readerContent.setPrev(readerCCID);
        readerCCID.setNext(readerContent);

        var cDateVar = new PropertyVariable(new NodeVariable("c", commentKey), "creationDate",
                DataType.INT);
        var cDateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(1338552000));
        var filterCreationDate = new Filter(cDateLessThanVal);
        readerCCID.setPrev(filterCreationDate);
        filterCreationDate.setNext(readerCCID);

        var readerCDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        filterCreationDate.setPrev(readerCDate);
        readerCDate.setNext(filterCreationDate);

        var flattenAndExtendToC = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p2", "person", "c", "comment", "hC", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerCDate.setPrev(flattenAndExtendToC);
        flattenAndExtendToC.setNext(readerCDate);

        var readerPFName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "fName");
        flattenAndExtendToC.setPrev(readerPFName);
        readerPFName.setNext(flattenAndExtendToC);

        var readerPLName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "lName");
        readerPFName.setPrev(readerPLName);
        readerPLName.setNext(readerPFName);

        var readerPCID = makeNodePropReader(graph, "p2", "person", DataType.INT, "cid");
        readerPLName.setPrev(readerPCID);
        readerPCID.setNext(readerPLName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCID);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p2:person)-[:knows]->(p3:person)-[:isLocatedIn]->(city:place),
     * (p3:person)<-[:hasCreator]-(mX)-[:isLocatedIn]->(countryX:place),
     * (p3:person)<-[:hasCreator]-(mY)-[:isLocatedIn]->(countryY:place)
     * WHERE person.id = 22468883 AND
     * mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND
     * mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 AND
     * countryX.name = 'India' AND countryY.name = 'China'
     */
    public static Operator IC03(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");
        var commentKey = catalog.getTypeKey("comment");
        var placeKey = catalog.getTypeKey("place");

        var sink = new SinkCount();

        var cYNameVar = new PropertyVariable(new NodeVariable("cY", placeKey), "name", DataType.STRING);
        var cYNameEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cYNameVar, new StringLiteral("China"));
        var cYFilter = new Filter(cYNameEqualsTo);
        sink.setPrev(cYFilter);
        cYFilter.setNext(sink);

        var readerCYName = makeNodePropReader(graph, "cY", "place", DataType.STRING, "name");
        cYFilter.setPrev(readerCYName);
        readerCYName.setNext(cYFilter);

        var extendToCY = new ExtendColumn(makeALD(catalog,
                "mY", "comment", "cY", "place", "l3", "isLocatedIn", Direction.FORWARD));
        readerCYName.setPrev(extendToCY);
        extendToCY.setNext(readerCYName);

        var cXNameVar = new PropertyVariable(new NodeVariable("cX", placeKey), "name", DataType.STRING);
        var cXNameEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cXNameVar,
                new StringLiteral("India"));
        var cXFilter = new Filter(cXNameEqualsTo);
        extendToCY.setPrev(cXFilter);
        cXFilter.setNext(extendToCY);

        var readerCXName = makeNodePropReader(graph, "cX", "place", DataType.STRING, "name");
        cXFilter.setPrev(readerCXName);
        readerCXName.setNext(cXFilter);

        var extendToCX = new ExtendColumn(makeALD(catalog,
                "mX", "comment", "cX", "place", "l2", "isLocatedIn", Direction.FORWARD));
        readerCXName.setPrev(extendToCX);
        extendToCX.setNext(readerCXName);
        var mYDateVar = new PropertyVariable(new NodeVariable("mY", commentKey), "creationDate",
                DataType.INT);
        var mYDateGreaterThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.GREATER_THAN_OR_EQUAL, mYDateVar, new IntLiteral(1313591219));
        var mYDateLessThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.LESS_THAN_OR_EQUAL, mYDateVar, new IntLiteral(1513591219));
        var mYDatePredicate = new BooleanConnectorExpression.ANDExpression(
                mYDateGreaterThanOrEqualToVal, mYDateLessThanOrEqualToVal);

        var filterMYCreationDate = new Filter(mYDatePredicate);
        extendToCX.setPrev(filterMYCreationDate);
        filterMYCreationDate.setNext(extendToCX);

        var readerMYDate = makeNodePropReader(graph, "mY", "comment", DataType.INT, "creationDate");
        filterMYCreationDate.setPrev(readerMYDate);
        readerMYDate.setNext(filterMYCreationDate);

        var extendToMY = new ExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "mY", "comment", "h2", "hasCreator", Direction.BACKWARD), commentKey);
        readerMYDate.setPrev(extendToMY);
        extendToMY.setNext(readerMYDate);

        var mXDateVar = new PropertyVariable(new NodeVariable("mX", commentKey), "creationDate",
                DataType.INT);
        var mXDateGreaterThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.GREATER_THAN_OR_EQUAL, mXDateVar, new IntLiteral(1313591219));
        var mXDateLessThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.LESS_THAN_OR_EQUAL, mXDateVar, new IntLiteral(1513591219));
        var mXDatePredicate = new BooleanConnectorExpression.ANDExpression(
                mXDateGreaterThanOrEqualToVal, mXDateLessThanOrEqualToVal);
        var filterMXCreationDate = new Filter(mXDatePredicate);
        extendToMY.setPrev(filterMXCreationDate);
        filterMXCreationDate.setNext(extendToMY);

        var readerMXDate = makeNodePropReader(graph, "mX", "comment", DataType.INT, "creationDate");
        filterMXCreationDate.setPrev(readerMXDate);
        readerMXDate.setNext(filterMXCreationDate);

        var extendToMX = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "mX", "comment", "h1", "hasCreator", Direction.BACKWARD), commentKey);
        readerMXDate.setPrev(extendToMX);
        extendToMX.setNext(readerMXDate);

        var flattenAndExtendToPlace = new ExtendColumn(makeALD(catalog,
                "p3", "person", "city", "place", "l1", "isLocatedIn", Direction.FORWARD));
        extendToMX.setPrev(flattenAndExtendToPlace);
        flattenAndExtendToPlace.setNext(extendToMX);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToPlace.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToPlace);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p3:person)-[:isLocatedIn]->(city:place),
     * (p3:person)<-[:hasCreator]-(mX)-[:isLocatedIn]->(countryX:place),
     * (p3:person)<-[:hasCreator]-(mY)-[:isLocatedIn]->(countryY:place)
     * WHERE person.id = 22468883 AND
     * mX.creationDate >= 1313591219 AND mX.creationDate <= 1513591219 AND
     * mY.creationDate >= 1313591219 AND mY.creationDate <= 1513591219 AND
     * countryX.name = 'India' AND countryY.name = 'China'
     */
    public static Operator IC03A(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");
        var commentKey = catalog.getTypeKey("comment");
        var placeKey = catalog.getTypeKey("place");

        var sink = new SinkCount();

        var cYNameVar = new PropertyVariable(new NodeVariable("cY", placeKey), "name", DataType.STRING);
        var cYNameEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cYNameVar, new StringLiteral("China"));
        var cYFilter = new Filter(cYNameEqualsTo);
        sink.setPrev(cYFilter);
        cYFilter.setNext(sink);

        var readerCYName = makeNodePropReader(graph, "cY", "place", DataType.STRING, "name");
        cYFilter.setPrev(readerCYName);
        readerCYName.setNext(cYFilter);

        var extendToCY = new ExtendColumn(makeALD(catalog,
                "mY", "comment", "cY", "place", "l3", "isLocatedIn", Direction.FORWARD));
        readerCYName.setPrev(extendToCY);
        extendToCY.setNext(readerCYName);

        var cXNameVar = new PropertyVariable(new NodeVariable("cX", placeKey), "name", DataType.STRING);
        var cXNameEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cXNameVar,
                new StringLiteral("India"));
        var cXFilter = new Filter(cXNameEqualsTo);
        extendToCY.setPrev(cXFilter);
        cXFilter.setNext(extendToCY);

        var readerCXName = makeNodePropReader(graph, "cX", "place", DataType.STRING, "name");
        cXFilter.setPrev(readerCXName);
        readerCXName.setNext(cXFilter);

        var extendToCX = new ExtendColumn(makeALD(catalog,
                "mX", "comment", "cX", "place", "l2", "isLocatedIn", Direction.FORWARD));
        readerCXName.setPrev(extendToCX);
        extendToCX.setNext(readerCXName);
        var mYDateVar = new PropertyVariable(new NodeVariable("mY", commentKey), "creationDate",
                DataType.INT);
        var mYDateGreaterThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.GREATER_THAN_OR_EQUAL, mYDateVar, new IntLiteral(1313591219));
        var mYDateLessThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.LESS_THAN_OR_EQUAL, mYDateVar, new IntLiteral(1513591219));
        var mYDatePredicate = new BooleanConnectorExpression.ANDExpression(
                mYDateGreaterThanOrEqualToVal, mYDateLessThanOrEqualToVal);

        var filterMYCreationDate = new Filter(mYDatePredicate);
        extendToCX.setPrev(filterMYCreationDate);
        filterMYCreationDate.setNext(extendToCX);

        var readerMYDate = makeNodePropReader(graph, "mY", "comment", DataType.INT, "creationDate");
        filterMYCreationDate.setPrev(readerMYDate);
        readerMYDate.setNext(filterMYCreationDate);

        var extendToMY = new ExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "mY", "comment", "h2", "hasCreator", Direction.BACKWARD), commentKey);
        readerMYDate.setPrev(extendToMY);
        extendToMY.setNext(readerMYDate);

        var mXDateVar = new PropertyVariable(new NodeVariable("mX", commentKey), "creationDate",
                DataType.INT);
        var mXDateGreaterThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.GREATER_THAN_OR_EQUAL, mXDateVar, new IntLiteral(1313591219));
        var mXDateLessThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.LESS_THAN_OR_EQUAL, mXDateVar, new IntLiteral(1513591219));
        var mXDatePredicate = new BooleanConnectorExpression.ANDExpression(
                mXDateGreaterThanOrEqualToVal, mXDateLessThanOrEqualToVal);
        var filterMXCreationDate = new Filter(mXDatePredicate);
        extendToMY.setPrev(filterMXCreationDate);
        filterMXCreationDate.setNext(extendToMY);

        var readerMXDate = makeNodePropReader(graph, "mX", "comment", DataType.INT, "creationDate");
        filterMXCreationDate.setPrev(readerMXDate);
        readerMXDate.setNext(filterMXCreationDate);

        var extendToMX = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "mX", "comment", "h1", "hasCreator", Direction.BACKWARD), commentKey);
        readerMXDate.setPrev(extendToMX);
        extendToMX.setNext(readerMXDate);

        var flattenAndExtendToPlace = new ExtendColumn(makeALD(catalog,
                "p3", "person", "city", "place", "l1", "isLocatedIn", Direction.FORWARD));
        extendToMX.setPrev(flattenAndExtendToPlace);
        flattenAndExtendToPlace.setNext(extendToMX);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p3", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToPlace.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToPlace);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:Person)<-[:knows]-(p2:Person)-[:knows]->(p3:Person),
     * (p3:Person)<-[:hasCreator]-(pst:Post)-[:hasTag]->(tag:Tag)
     * WHERE p1.id = 22468883 AND post.creationDate >= 1313591219 AND
     * post.creationDate <= 1513591219
     * RETURN tag.name
     */
    public static Operator IC04(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");
        var postKey = catalog.getTypeKey("post");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"t.name"}  /* str Vectors Lists */);

        var readerContent = makeNodePropReader(graph, "t", "tag", DataType.STRING, "name");
        sink.setPrev(readerContent);
        readerContent.setNext(sink);

        var flattenAndExtendToTag = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "pst", "post", "t", "tag", "h2", "hasTag", Direction.FORWARD));
        readerContent.setPrev(flattenAndExtendToTag);
        flattenAndExtendToTag.setNext(readerContent);

        var postDateVar = new PropertyVariable(new NodeVariable("pst", postKey), "creationDate",
                DataType.INT);
        var postDateGreaterThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.GREATER_THAN_OR_EQUAL, postDateVar, new IntLiteral(1313591219));
        var postDateLessThanOrEqualToVal = new ComparisonExpression(
                ComparisonOperator.LESS_THAN_OR_EQUAL, postDateVar, new IntLiteral(1513591219));
        var postDatePredicate = new BooleanConnectorExpression.ANDExpression(
                postDateGreaterThanOrEqualToVal, postDateLessThanOrEqualToVal);
        var filterPostCreationDate = new Filter(postDatePredicate);
        flattenAndExtendToTag.setPrev(filterPostCreationDate);
        filterPostCreationDate.setNext(flattenAndExtendToTag);

        var readerPostDate = makeNodePropReader(graph, "pst", "post", DataType.INT, "creationDate");
        filterPostCreationDate.setPrev(readerPostDate);
        readerPostDate.setNext(filterPostCreationDate);

        var extendP3ToPost = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "pst", "post", "h1", "hasCreator", Direction.BACKWARD), postKey);
        readerPostDate.setPrev(extendP3ToPost);
        extendP3ToPost.setNext(readerPostDate);

        var extendToP3 = new ExtendAdjListsNoSelReset(
                makeALD(catalog, "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        extendP3ToPost.setPrev(extendToP3);
        extendToP3.setNext(extendP3ToPost);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
                makeALD(catalog, "p2", "person", "p1", "person", "k1", "knows", Direction.FORWARD));
        extendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(extendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p2", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p2", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p2", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p2:person)-[:knows]->(p3:person),
     * (p3:person)<-[e:hasMember]-(f:forum)-[:containerOf]->(p:post)
     * WHERE p1 = 22468883 AND e.date > 1267302820
     */
    public static Operator IC05(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{"f.title"}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var flattenAndExtendToP = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "f", "forum", "p", "post", "k3", "containerOf", Direction.FORWARD));
        sink.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(sink);

        var readerPostDate = makeNodePropReader(graph, "f", "forum", DataType.STRING, "title");
        flattenAndExtendToP.setPrev(readerPostDate);
        readerPostDate.setNext(flattenAndExtendToP);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p3", "person", "f", "forum", "e", "hasMember", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar,
                new IntLiteral(1353819600));
        var filterDate = new Filter(dateGreaterThanVal);
        readerPostDate.setPrev(filterDate);
        filterDate.setNext(readerPostDate);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var flattenAndExtendToF = new FlattenAndExtendAdjLists(makeALD(catalog,
                "p3", "person", "f", "forum", "e", "hasMember", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(readerDate);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToF.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToF);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p3:person),
     * (p3:person)<-[e:hasMember]-(f:forum)-[:containerOf]->(p:post)
     * WHERE p1 = 22468883 AND e.date > 1267302820
     */
    public static Operator IC05A(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{"f.title"}, /* str Vectors Flat */
                new String[]{}  /* str Vectors Lists */);

        var flattenAndExtendToP = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "f", "forum", "p", "post", "k3", "containerOf", Direction.FORWARD));
        sink.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(sink);

        var readerPostDate = makeNodePropReader(graph, "f", "forum", DataType.STRING, "title");
        flattenAndExtendToP.setPrev(readerPostDate);
        readerPostDate.setNext(flattenAndExtendToP);

        var relVar = makeRelVar(graph.getGraphCatalog(),
                "p3", "person", "f", "forum", "e", "hasMember", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar,
                new IntLiteral(1353819600));
        var filterDate = new Filter(dateGreaterThanVal);
        readerPostDate.setPrev(filterDate);
        filterDate.setNext(readerPostDate);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var flattenAndExtendToF = new FlattenAndExtendAdjLists(makeALD(catalog,
                "p3", "person", "f", "forum", "e", "hasMember", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(readerDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p3", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToF.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToF);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p2:person)-[:knows]->(p3:person)<-[:hasCreator]-(pst:post),
     * (pst:post)-[:hasTag]->(t:tag),
     * (pst:post)-[:hasTag]->(ot:tag)
     * WHERE p1.id = 22468883 AND t.name='Rumi' AND ot.name<>'Rumi'
     * RETURN otherTag.name
     */
    public static Operator IC06(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"ot.name"}  /* str Vectors Lists */);

        var nameVar = new PropertyVariable(new NodeVariable("ot", catalog.getTypeKey("tag")),
                "name", DataType.STRING);
        var tagClassNameEqualToPerson = new ComparisonExpression(ComparisonOperator.NOT_EQUALS,
                nameVar, new StringLiteral("Rumi"));
        var filterOT = new Filter(tagClassNameEqualToPerson);
        sink.setPrev(filterOT);
        filterOT.setNext(sink);

        var readerOTagClassName = makeNodePropReader(graph, "ot", "tag", DataType.STRING, "name");
        filterOT.setPrev(readerOTagClassName);
        readerOTagClassName.setNext(filterOT);

        var flattenAndExtendToOT = new ExtendAdjLists(makeALD(catalog,
                "pst", "post", "ot", "tag", "k4", "hasTag", Direction.FORWARD));
        readerOTagClassName.setPrev(flattenAndExtendToOT);
        flattenAndExtendToOT.setNext(readerOTagClassName);

        var nameTVar = new PropertyVariable(new NodeVariable("t", catalog.getTypeKey("tag")),
                "name", DataType.STRING);
        var tagClassNameTEqualToPerson = new ComparisonExpression(ComparisonOperator.EQUALS,
                nameTVar, new StringLiteral("Rumi"));
        var filterNameEqualToPerson = new Filter(tagClassNameTEqualToPerson);
        flattenAndExtendToOT.setPrev(filterNameEqualToPerson);
        filterNameEqualToPerson.setNext(flattenAndExtendToOT);

        var readerTagClassName = makeNodePropReader(graph, "t", "tag", DataType.STRING, "name");
        filterNameEqualToPerson.setPrev(readerTagClassName);
        readerTagClassName.setNext(filterNameEqualToPerson);

        var flattenAndExtendToT = new FlattenAndExtendAdjLists(makeALD(catalog,
                "pst", "post", "t", "tag", "k3", "hasTag", Direction.FORWARD));
        readerTagClassName.setPrev(flattenAndExtendToT);
        flattenAndExtendToT.setNext(readerTagClassName);

        var flattenAndExtendToP = new FlattenAndExtendAdjListsNoSelResetWithType(makeALD(catalog,
                "p3", "person", "pst", "post", "hC", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("post"));
        flattenAndExtendToT.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(flattenAndExtendToT);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToP.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToP);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p3:person)<-[:hasCreator]-(pst:post),
     * (pst:post)-[:hasTag]->(t:tag),
     * (pst:post)-[:hasTag]->(ot:tag)
     * WHERE p1.id = 22468883 AND t.name='Rumi' AND ot.name<>'Rumi'
     * RETURN otherTag.name
     */
    public static Operator IC06A(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"ot.name"}  /* str Vectors Lists */);

        var nameVar = new PropertyVariable(new NodeVariable("ot", catalog.getTypeKey("tag")),
                "name", DataType.STRING);
        var tagClassNameEqualToPerson = new ComparisonExpression(ComparisonOperator.NOT_EQUALS,
                nameVar, new StringLiteral("Rumi"));
        var filterOT = new Filter(tagClassNameEqualToPerson);
        sink.setPrev(filterOT);
        filterOT.setNext(sink);

        var readerOTagClassName = makeNodePropReader(graph, "ot", "tag", DataType.STRING, "name");
        filterOT.setPrev(readerOTagClassName);
        readerOTagClassName.setNext(filterOT);

        var flattenAndExtendToOT = new ExtendAdjLists(makeALD(catalog,
                "pst", "post", "ot", "tag", "k4", "hasTag", Direction.FORWARD));
        readerOTagClassName.setPrev(flattenAndExtendToOT);
        flattenAndExtendToOT.setNext(readerOTagClassName);

        var nameTVar = new PropertyVariable(new NodeVariable("t", catalog.getTypeKey("tag")),
                "name", DataType.STRING);
        var tagClassNameTEqualToPerson = new ComparisonExpression(ComparisonOperator.EQUALS,
                nameTVar, new StringLiteral("Rumi"));
        var filterNameEqualToPerson = new Filter(tagClassNameTEqualToPerson);
        flattenAndExtendToOT.setPrev(filterNameEqualToPerson);
        filterNameEqualToPerson.setNext(flattenAndExtendToOT);

        var readerTagClassName = makeNodePropReader(graph, "t", "tag", DataType.STRING, "name");
        filterNameEqualToPerson.setPrev(readerTagClassName);
        readerTagClassName.setNext(filterNameEqualToPerson);

        var flattenAndExtendToT = new FlattenAndExtendAdjLists(makeALD(catalog,
                "pst", "post", "t", "tag", "k3", "hasTag", Direction.FORWARD));
        readerTagClassName.setPrev(flattenAndExtendToT);
        flattenAndExtendToT.setNext(readerTagClassName);

        var flattenAndExtendToP = new FlattenAndExtendAdjListsNoSelResetWithType(makeALD(catalog,
                "p3", "person", "pst", "post", "hC", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("post"));
        flattenAndExtendToT.setPrev(flattenAndExtendToP);
        flattenAndExtendToP.setNext(flattenAndExtendToT);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p3", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (person:Person)<-[:hasCreator]-(c:comment)<-[l:likes]-(friend:Person)
     * WHERE person.id = 22468883
     * RETURN p2.id, p2.fName, p2.lName, l.date, c.content
     */
    public static Operator IC07(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"p2.cid", "l.date"} /* int Vectors Lists */,
                new String[]{"c.content"}, /* str Vectors Flat */
                new String[]{"p2.fName", "p2.lName"}  /* str Vectors Lists */);

        var readerPFName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "fName");
        sink.setPrev(readerPFName);
        readerPFName.setNext(sink);

        var readerPLName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "lName");
        readerPFName.setPrev(readerPLName);
        readerPLName.setNext(readerPFName);

        var readerContent = makeNodePropReader(graph, "p2", "person", DataType.INT, "cid");
        readerPLName.setPrev(readerContent);
        readerContent.setNext(readerPLName);

        var relVar = makeRelVar(graph.getGraphCatalog(), "c", "comment", "p2", "person", "l",
                "likes", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        readerContent.setPrev(readerDate);
        readerDate.setNext(readerContent);

        var flattenAndExtendToF = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "c", "comment", "p2", "person", "l", "likes", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(readerDate);

        var readerOTagClassName = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        flattenAndExtendToF.setPrev(readerOTagClassName);
        readerOTagClassName.setNext(flattenAndExtendToF);

        var flattenAndExtendToC = new FlattenAndExtendAdjListsNoSelResetWithType(makeALD(catalog,
                "p1", "person", "c", "comment", "hC", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerOTagClassName.setPrev(flattenAndExtendToC);
        flattenAndExtendToC.setNext(readerOTagClassName);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToC.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToC);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p:Person)<-[:hasCreator]-(pst:post)<-[:replyOf]-(c:Comment),
     * (c:Comment)-[:hasCreator]->(cAuthor:Person)
     * WHERE person.id = 22468883
     * RETURN cmtAuthor.id, cmtAuthor.fName, cmtAuthor.lName, c.creationDate, c.id, c.content
     */
    public static Operator IC08(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{}, // ,/*int Vectors Flat */,
                new String[]{"cAuthor.cid", "c.creationDate", "c.cid"} /* int Vectors Lists */,
                new String[]{}, /* str Vectors Flat */
                new String[]{"cAuthor.fName", "cAuthor.lName", "c.content"}  /*str Vectors Lists*/);

        var readerCCID = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        sink.setPrev(readerCCID);
        readerCCID.setNext(sink);

        var readerCDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        readerCCID.setPrev(readerCDate);
        readerCDate.setNext(readerCCID);

        var readerContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        readerCDate.setPrev(readerContent);
        readerContent.setNext(readerCDate);

        var readerPFName = makeNodePropReader(graph, "cAuthor", "person", DataType.STRING, "fName");
        readerContent.setPrev(readerPFName);
        readerPFName.setNext(readerContent);

        var readerPLName = makeNodePropReader(graph, "cAuthor", "person", DataType.STRING, "lName");
        readerPFName.setPrev(readerPLName);
        readerPLName.setNext(readerPFName);

        var readerPCID = makeNodePropReader(graph, "cAuthor", "person", DataType.INT, "cid");
        readerPLName.setPrev(readerPCID);
        readerPCID.setNext(readerPLName);

        var extendToCAuthor = new ExtendColumn(makeALD(catalog,
                "c", "comment", "cAuthor", "person", "hC2", "hasCreator", Direction.FORWARD));
        readerPCID.setPrev(extendToCAuthor);
        extendToCAuthor.setNext(readerPCID);

        var extendToPst = new FlattenAndExtendAdjLists(makeALD(catalog,
                "pst", "post", "c", "comment", "rO", "replyOf", Direction.BACKWARD));
        extendToCAuthor.setPrev(extendToPst);
        extendToPst.setNext(extendToCAuthor);

        var flattenAndExtendToC = new FlattenAndExtendAdjListsNoSelResetWithType(
                makeALD(catalog, "p", "person", "pst", "post", "hC1", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("post"));
        extendToPst.setPrev(flattenAndExtendToC);
        flattenAndExtendToC.setNext(extendToPst);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToC.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToC);

        var reader = makeNodePropReaderWithOffset(graph, "p", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:Person)-[:knows]->(p2:Person)-[:knows]->(p3:Person)<-[:hasCreator]-(c:comment)
     * WHERE p1.id = 22468883 AND c.creationDate < 1342840042
     * RETURN p3.id, p3.fName, p3.lName, c.id, c.content, c.creationDate
     */
    public static Operator IC09(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{"p3.cid"}, // ,/*int Vectors Flat */,
                new String[]{"c.creationDate", "c.cid"} /* int Vectors Lists */,
                new String[]{"p3.fName", "p3.lName"}, /* str Vectors Flat */
                new String[]{"c.content"}  /*str Vectors Lists*/);

        var readerCCID = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        sink.setPrev(readerCCID);
        readerCCID.setNext(sink);

        var readerContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        readerCCID.setPrev(readerContent);
        readerContent.setNext(readerCCID);

        var cDateVar = new PropertyVariable(new NodeVariable("c", catalog.getTypeKey("comment")),
                "creationDate", DataType.INT);
        var cDateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(1342840042));
        var filterCreationDate = new Filter(cDateLessThanVal);
        readerContent.setPrev(filterCreationDate);
        filterCreationDate.setNext(readerContent);

        var readerCDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        filterCreationDate.setPrev(readerCDate);
        readerCDate.setNext(filterCreationDate);

        var flattenAndExtendToF = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "c", "comment", "e", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerCDate.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(readerCDate);

        var readerPFName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "fName");
        flattenAndExtendToF.setPrev(readerPFName);
        readerPFName.setNext(flattenAndExtendToF);

        var readerPLName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "lName");
        readerPFName.setPrev(readerPLName);
        readerPLName.setNext(readerPFName);

        var readerPCID = makeNodePropReader(graph, "p3", "person", DataType.INT, "cid");
        readerPLName.setPrev(readerPCID);
        readerPCID.setNext(readerPLName);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerPCID);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:Person)-[:knows]->(p3:Person)<-[:hasCreator]-(c:comment)
     * WHERE p1.id = 22468883 AND c.creationDate < 1342840042
     * RETURN p3.id, p3.fName, p3.lName, c.id, c.content, c.creationDate
     */
    public static Operator IC09A(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{"p3.cid"}, // ,/*int Vectors Flat */,
                new String[]{"c.creationDate", "c.cid"} /* int Vectors Lists */,
                new String[]{"p3.fName", "p3.lName"}, /* str Vectors Flat */
                new String[]{"c.content"}  /*str Vectors Lists*/);

        var readerCCID = makeNodePropReader(graph, "c", "comment", DataType.INT, "cid");
        sink.setPrev(readerCCID);
        readerCCID.setNext(sink);

        var readerContent = makeNodePropReader(graph, "c", "comment", DataType.STRING, "content");
        readerCCID.setPrev(readerContent);
        readerContent.setNext(readerCCID);

        var cDateVar = new PropertyVariable(new NodeVariable("c", catalog.getTypeKey("comment")),
                "creationDate", DataType.INT);
        var cDateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(1342840042));
        var filterCreationDate = new Filter(cDateLessThanVal);
        readerContent.setPrev(filterCreationDate);
        filterCreationDate.setNext(readerContent);

        var readerCDate = makeNodePropReader(graph, "c", "comment", DataType.INT, "creationDate");
        filterCreationDate.setPrev(readerCDate);
        readerCDate.setNext(filterCreationDate);

        var flattenAndExtendToF = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p3", "person", "c", "comment", "e", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        readerCDate.setPrev(flattenAndExtendToF);
        flattenAndExtendToF.setNext(readerCDate);

        var readerPFName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "fName");
        flattenAndExtendToF.setPrev(readerPFName);
        readerPFName.setNext(flattenAndExtendToF);

        var readerPLName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "lName");
        readerPFName.setPrev(readerPLName);
        readerPLName.setNext(readerPFName);

        var readerPCID = makeNodePropReader(graph, "p3", "person", DataType.INT, "cid");
        readerPLName.setPrev(readerPCID);
        readerPCID.setNext(readerPLName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p3", "person", "k1", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCID);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    /**
     * MATCH (p1:Person)-[:knows]->(p2:Person)-[:knows]->(p3:Person)-[w:workAt]->(org:Organisation),
     * (org:Organisation)-[:isLocatedIn]->(pl:Place)
     * WHERE p1.id = 22468883 AND e.year < 2016 AND pl.name = 'China'
     * RETURN p3.id, p3.fName, p3.lName, org.name
     */
    public static Operator IC11(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{"p3.cid"}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{"p3.fName", "p3.lName"}, /* str Vectors Flat */
                new String[]{"org.name"}  /*str Vectors Lists*/);

        var nameVar = new PropertyVariable(new NodeVariable("pl", catalog.getTypeKey("place")),
                "name", DataType.STRING);
        var nameEqualToJohn = new ComparisonExpression(ComparisonOperator.EQUALS, nameVar,
                new StringLiteral("China"));
        var filterFNameEqualToChina = new FilterNoAnd(nameEqualToJohn);
        sink.setPrev(filterFNameEqualToChina);
        filterFNameEqualToChina.setNext(sink);

        var readerP4FName = makeNodePropReader(graph, "pl", "place", DataType.STRING, "name");
        filterFNameEqualToChina.setPrev(readerP4FName);
        readerP4FName.setNext(filterFNameEqualToChina);

        var extendToPl = new ExtendColumn(makeALD(catalog, "org", "organisation", "pl",
                "place", "l", "isLocatedIn", Direction.FORWARD));
        readerP4FName.setPrev(extendToPl);
        extendToPl.setNext(readerP4FName);

        var relVar = makeRelVar(graph.getGraphCatalog(), "p3", "person", "org", "organisation",
                "w", "workAt", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "year", DataType.INT);
        var cDateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(2016));
        var filterYear = new Filter(cDateLessThanVal);
        extendToPl.setPrev(filterYear);
        filterYear.setNext(extendToPl);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterYear.setPrev(readerDate);
        readerDate.setNext(filterYear);

        var reader = makeNodePropReader(graph, "org", "organisation", DataType.STRING, "name");
        readerDate.setPrev(reader);
        reader.setNext(readerDate);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(makeALD(catalog,
                "p3", "person", "org", "organisation", "w", "workAt", Direction.FORWARD));
        reader.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(reader);

        var readerLName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "lName");
        flattenAndExtendToP4.setPrev(readerLName);
        readerLName.setNext(flattenAndExtendToP4);

        var readerFName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var readerPCID = makeNodePropReader(graph, "p3", "person", DataType.INT, "cid");
        readerFName.setPrev(readerPCID);
        readerPCID.setNext(readerFName);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p2", "person", "p3", "person", "k2", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerPCID);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var readerP1CID = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(readerP1CID);
        readerP1CID.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        readerP1CID.setPrev(scanP1);
        scanP1.setNext(readerP1CID);

        return sink;
    }

    /**
     * MATCH (p1:Person)-[:knows]->(p3:Person)-[w:workAt]->(org:Organisation),
     * (org:Organisation)-[:isLocatedIn]->(pl:Place)
     * WHERE p1.id = 22468883 AND e.year < 2016 AND pl.name = 'China'
     * RETURN p3.id, p3.fName, p3.lName, org.name
     */
    public static Operator IC11A(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("person");

        var sink = new SinkCopy(
                new String[]{"p3.cid"}, // ,/*int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{"p3.fName", "p3.lName"}, /* str Vectors Flat */
                new String[]{"org.name"}  /*str Vectors Lists*/);

        var nameVar = new PropertyVariable(new NodeVariable("pl", catalog.getTypeKey("place")),
                "name", DataType.STRING);
        var nameEqualToJohn = new ComparisonExpression(ComparisonOperator.EQUALS, nameVar,
                new StringLiteral("China"));
        var filterFNameEqualToChina = new FilterNoAnd(nameEqualToJohn);
        sink.setPrev(filterFNameEqualToChina);
        filterFNameEqualToChina.setNext(sink);

        var readerP4FName = makeNodePropReader(graph, "pl", "place", DataType.STRING, "name");
        filterFNameEqualToChina.setPrev(readerP4FName);
        readerP4FName.setNext(filterFNameEqualToChina);

        var extendToPl = new ExtendColumn(makeALD(catalog, "org", "organisation", "pl",
                "place", "l", "isLocatedIn", Direction.FORWARD));
        readerP4FName.setPrev(extendToPl);
        extendToPl.setNext(readerP4FName);

        var relVar = makeRelVar(graph.getGraphCatalog(), "p3", "person", "org", "organisation",
                "w", "workAt", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "year", DataType.INT);
        var cDateLessThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
                new IntLiteral(2016));
        var filterYear = new Filter(cDateLessThanVal);
        extendToPl.setPrev(filterYear);
        filterYear.setNext(extendToPl);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
                graph.getGraphCatalog());
        filterYear.setPrev(readerDate);
        readerDate.setNext(filterYear);

        var reader = makeNodePropReader(graph, "org", "organisation", DataType.STRING, "name");
        readerDate.setPrev(reader);
        reader.setNext(readerDate);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjLists(makeALD(catalog,
                "p3", "person", "org", "organisation", "w", "workAt", Direction.FORWARD));
        reader.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(reader);

        var readerLName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "lName");
        flattenAndExtendToP4.setPrev(readerLName);
        readerLName.setNext(flattenAndExtendToP4);

        var readerFName = makeNodePropReader(graph, "p3", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var readerPCID = makeNodePropReader(graph, "p3", "person", DataType.INT, "cid");
        readerFName.setPrev(readerPCID);
        readerPCID.setNext(readerFName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p3", "person", "k1", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCID);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var readerP1CID = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(readerP1CID);
        readerP1CID.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        readerP1CID.setPrev(scanP1);
        scanP1.setNext(readerP1CID);

        return sink;
    }

    /**
     * MATCH (p1:person)-[:knows]->(p2:person)<-[:hasCreator]-(c:comment),
     * (c:comment)-[:replyOf]->(pst:post)-[:hasTag]->(t:tag),
     * (t:tag)-[:hasType]->(tc:tagclass)-[:isSubclassOf]->(tsc:tagclass)
     * WHERE p1.id = 22468883 AND tsc.name='Person'
     * RETURN p2.id, p2.fName, p2.lName
     */
    public static Operator IC12(Graph graph) {
        var catalog = graph.getGraphCatalog();

        var sink = new SinkCopy(
                new String[]{"p2.cid"} /* int Vectors Flat */,
                new String[]{} /* int Vectors Lists */,
                new String[]{"p2.fName", "p2.lName"}, /* str Vectors Flat */
                new String[]{}  /*str Vectors Lists*/);

        var nameVar = new PropertyVariable(new NodeVariable("tsc", catalog.getTypeKey("tagclass")),
                "name", DataType.STRING);
        var tagClassNameEqualToPerson = new ComparisonExpression(ComparisonOperator.EQUALS, nameVar,
                new StringLiteral("Person"));
        var filterNameEqualToPerson = new Filter(tagClassNameEqualToPerson);
        sink.setPrev(filterNameEqualToPerson);
        filterNameEqualToPerson.setNext(sink);

        var readerTagClassName = makeNodePropReader(graph, "tsc", "tagclass", DataType.STRING, "name");
        filterNameEqualToPerson.setPrev(readerTagClassName);
        readerTagClassName.setNext(filterNameEqualToPerson);

        var flattenAndExtendToTSC = new ExtendColumn(makeALD(catalog,
                "tc", "tagclass", "tsc", "tagclass", "k6", "isSubclassOf", Direction.FORWARD));
        readerTagClassName.setPrev(flattenAndExtendToTSC);
        flattenAndExtendToTSC.setNext(readerTagClassName);

        var flattenAndExtendToTC = new ExtendColumn(makeALD(catalog,
                "t", "tag", "tc", "tagclass", "k5", "hasType", Direction.FORWARD));
        flattenAndExtendToTSC.setPrev(flattenAndExtendToTC);
        flattenAndExtendToTC.setNext(flattenAndExtendToTSC);

        var flattenAndExtendToT = new FlattenAndExtendAdjLists(makeALD(catalog,
                "pst", "post", "t", "tag", "k4", "hasTag", Direction.FORWARD));
        flattenAndExtendToTC.setPrev(flattenAndExtendToT);
        flattenAndExtendToT.setNext(flattenAndExtendToTC);

        var extendToPst = new ExtendColumnWithType(makeALD(catalog,
                "c", "comment", "pst", "post", "k3", "replyOf", Direction.FORWARD),
                catalog.getTypeKey("post"));
        flattenAndExtendToT.setPrev(extendToPst);
        extendToPst.setNext(flattenAndExtendToT);

        var flattenAndExtendToC = new FlattenAndExtendAdjListsWithType(makeALD(catalog,
                "p2", "person", "c", "comment", "k2", "hasCreator", Direction.BACKWARD),
                catalog.getTypeKey("comment"));
        extendToPst.setPrev(flattenAndExtendToC);
        flattenAndExtendToC.setNext(extendToPst);

        var readerLName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "lName");
        flattenAndExtendToC.setPrev(readerLName);
        readerLName.setNext(flattenAndExtendToC);

        var readerFName = makeNodePropReader(graph, "p2", "person", DataType.STRING, "fName");
        readerLName.setPrev(readerFName);
        readerFName.setNext(readerLName);

        var readerPCID = makeNodePropReader(graph, "p2", "person", DataType.INT, "cid");
        readerFName.setPrev(readerPCID);
        readerPCID.setNext(readerFName);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(makeALD(catalog,
                "p1", "person", "p2", "person", "k1", "knows", Direction.FORWARD));
        readerPCID.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerPCID);

        var personKey = catalog.getTypeKey("person");

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.EQUALS, cidVar,
                new IntLiteral(27275921));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = makeNodePropReaderWithOffset(graph, "p1", "person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", personKey));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static PropertyReader makeNodePropReader(Graph graph, String variable, String nodeType,
                                                    DataType dataType, String property) {
        return makeNodePropReader(graph, variable, nodeType, dataType, property, false);
    }

    public static PropertyReader makeNodePropReaderWithOffset(Graph graph, String variable,
                                                              String nodeType,
                                                              DataType dataType, String property) {
        return makeNodePropReader(graph, variable, nodeType, dataType, property, true);
    }

    public static PropertyReader makeNodePropReader(Graph graph, String variable, String nodeType,
                                                    DataType dataType, String property, boolean withOffset) {
        var typeKey = graph.getGraphCatalog().getTypeKey(nodeType);
        var propVar = new PropertyVariable(new NodeVariable(variable, typeKey), property, dataType);
        propVar.setPropertyKey(graph.getGraphCatalog().getNodePropertyKey(property));
        return withOffset ?
                NodePropertyWithOffsetReader.make(propVar, graph.getNodePropertyStore()) :
                NodePropertyReader.make(propVar, graph.getNodePropertyStore());
    }

    public static RelVariable makeRelVar(GraphCatalog catalog, String boundVarName,
                                         String boundVarLabel, String nbrVarName, String nbrVarLabel, String relVarName,
                                         String relVarLabel, Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        return direction == Direction.FORWARD ?
                new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar) :
                new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), nbrVar, boundVar);
    }

    public static AdjListDescriptor makeALD(GraphCatalog catalog, String boundVarName,
                                            String boundVarLabel, String nbrVarName, String nbrVarLabel, String relVarName,
                                            String relVarLabel, Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        var relVar = direction == Direction.FORWARD ?
                new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar) :
                new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), nbrVar, boundVar);
        return new AdjListDescriptor(relVar, boundVar, nbrVar, direction);
    }

}
