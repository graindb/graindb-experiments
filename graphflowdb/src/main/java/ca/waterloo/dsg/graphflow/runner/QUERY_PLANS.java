package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjListsNoSelReset;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextend.ExtendAdjListsNoSelReset;
import ca.waterloo.dsg.graphflow.plan.operator.filter.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.rel.RelPropertyReader.RelPropertyIntReader;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanAllNodes;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCopy;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCount;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;

import static ca.waterloo.dsg.graphflow.runner.LDBC_PLANS.makeALD;

public class QUERY_PLANS {

    public static Operator H2_F_WIKI(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar2 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cidVar2 = new PropertyVariable(new NodeVariable("p3", personKey), "cid", DataType.INT);
        var cidVarEqualsTo2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar2,
            new IntLiteral(val));
        var cidFilter2 = new Filter(cidVarEqualsTo2);
        sink.setPrev(cidFilter2);
        cidFilter2.setNext(sink);

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p3", "Person", DataType.INT, "cid");
        cidFilter2.setPrev(reader2);
        reader2.setNext(cidFilter2);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        reader2.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(reader2);

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

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT,
            "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H2_B_WIKI(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar2 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cidVar2 = new PropertyVariable(new NodeVariable("p3", personKey), "cid", DataType.INT);
        var cidVarEqualsTo2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar2,
            new IntLiteral(val));
        var cidFilter2 = new Filter(cidVarEqualsTo2);
        sink.setPrev(cidFilter2);
        cidFilter2.setNext(sink);

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p3", "Person", DataType.INT, "cid");
        cidFilter2.setPrev(reader2);
        reader2.setNext(cidFilter2);

        var cmp_k1_less_k2 = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar, cDateVar2);
        var filter_k1_less_k2 = new Filter(cmp_k1_less_k2);
        reader2.setPrev(filter_k1_less_k2);
        filter_k1_less_k2.setNext(reader2);

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

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT,
            "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H3_F_WIKI(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar3 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD);
        var cDateVar3 = new PropertyVariable(relVar3, "date", DataType.INT);

        var relVar2 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cidVar2 = new PropertyVariable(new NodeVariable("p4", personKey), "cid", DataType.INT);
        var cidVarEqualsTo2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar2,
            new IntLiteral(val));
        var cidFilter2 = new Filter(cidVarEqualsTo2);
        sink.setPrev(cidFilter2);
        cidFilter2.setNext(sink);

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p4", "Person", DataType.INT, "cid");
        cidFilter2.setPrev(reader2);
        reader2.setNext(cidFilter2);

        var cmp_k2_less_k3 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar2, cDateVar3);
        var filter_k2_less_k3 = new Filter(cmp_k2_less_k3);
        reader2.setPrev(filter_k2_less_k3);
        filter_k2_less_k3.setNext(reader2);

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

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H3_B_WIKI(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar3 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p3", "Person", "p4", "Person", "k3", "knows", Direction.BACKWARD);
        var cDateVar3 = new PropertyVariable(relVar3, "date", DataType.INT);

        var relVar2 = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p2", "Person", "p3", "Person", "k2", "knows", Direction.BACKWARD);
        var cDateVar2 = new PropertyVariable(relVar2, "date", DataType.INT);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var cidVar2 = new PropertyVariable(new NodeVariable("p4", personKey), "cid", DataType.INT);
        var cidVarEqualsTo2 = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar2,
            new IntLiteral(val));
        var cidFilter2 = new Filter(cidVarEqualsTo2);
        sink.setPrev(cidFilter2);
        cidFilter2.setNext(sink);

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p4", "Person", DataType.INT, "cid");
        cidFilter2.setPrev(reader2);
        reader2.setNext(cidFilter2);

        var cmp_k2_less_k3 = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar2, cDateVar3);
        var filter_k2_less_k3 = new Filter(cmp_k2_less_k3);
        reader2.setPrev(filter_k2_less_k3);
        filter_k2_less_k3.setNext(reader2);

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

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.BACKWARD));
        readerDate.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(readerDate);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar, new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    // COUNT(*) for LDBC100, Wiki, flickr.
    public static Operator H1_COUNT(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        sink.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(sink);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReader(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H2_COUNT(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        sink.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(sink);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val));
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReader(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H3_COUNT(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var flattenAndExtendToP4 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD));
        sink.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(sink);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToP4.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToP4);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReader(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    // COUNT(*) for LDBC100, Wiki, flickr.
    public static Operator H1_FILTER(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar,
            new IntLiteral(2147483646));
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

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReader(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H2_FILTER(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar,
            new IntLiteral(2147483646));
        var filterDate = new Filter(dateGreaterThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerDate2 = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
            graph.getGraphCatalog());
        filterDate.setPrev(readerDate2);
        readerDate2.setNext(filterDate);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        readerDate2.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(readerDate2);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReader(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H1_FILTER_P(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCopy(
            new String[] { "p1.cid"  }, // ,/*int Vectors Flat */,
            new String[] { "p2.cid" } /* int Vectors Lists */,
            new String[] { }, /* str Vectors Flat */
            new String[] { }  /* str Vectors Lists */);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
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

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p2", "Person", DataType.INT, "cid");
        readerDate.setPrev(reader2);
        reader2.setNext(readerDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        reader2.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(reader2);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT, "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H2_FILTER_P(Graph graph) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCopy(
            new String[] { "p1.cid", "p2.cid" }, // ,/*int Vectors Flat */,
            new String[] { "p3.cid" } /* int Vectors Lists */,
            new String[] { }, /* str Vectors Flat */
            new String[] { }  /* str Vectors Lists */);

        var reader3 = LDBC_PLANS.makeNodePropReader(graph, "p3", "Person", DataType.INT, "cid");
        sink.setPrev(reader3);
        reader3.setNext(sink);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        reader3.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(reader3);

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.LESS_THAN, cDateVar,
            new IntLiteral(1267302820));
        var filterDate = new Filter(dateGreaterThanVal);
        flattenAndExtendToP3.setPrev(filterDate);
        filterDate.setNext(flattenAndExtendToP3);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
            graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var reader2 = LDBC_PLANS.makeNodePropReader(graph, "p2", "Person", DataType.INT, "cid");
        readerDate.setPrev(reader2);
        reader2.setNext(readerDate);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        reader2.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(reader2);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(1000)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT,
            "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }

    public static Operator H3_FILTER(Graph graph, int val) {
        var catalog = graph.getGraphCatalog();
        var personKey = catalog.getTypeKey("Person");

        var sink = new SinkCount();

        var relVar = LDBC_PLANS.makeRelVar(graph.getGraphCatalog(),
            "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);
        var dateGreaterThanVal = new ComparisonExpression(ComparisonOperator.GREATER_THAN, cDateVar,
            new IntLiteral(2147483646));
        var filterDate = new Filter(dateGreaterThanVal);
        sink.setPrev(filterDate);
        filterDate.setNext(sink);

        var readerDate = RelPropertyIntReader.make(cDateVar, graph.getRelPropertyStore(),
            graph.getGraphCatalog());
        filterDate.setPrev(readerDate);
        readerDate.setNext(filterDate);

        var flattenAndExtendToP4 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p3", "Person", "p4", "Person", "k3", "knows", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtendToP4);
        flattenAndExtendToP4.setNext(readerDate);

        var flattenAndExtendToP3 = new FlattenAndExtendAdjListsNoSelReset(
            makeALD(catalog, "p2", "Person", "p3", "Person", "k2", "knows", Direction.FORWARD));
        flattenAndExtendToP4.setPrev(flattenAndExtendToP3);
        flattenAndExtendToP3.setNext(flattenAndExtendToP4);

        var flattenAndExtendToP2 = new FlattenAndExtendAdjLists(
            makeALD(catalog, "p1", "Person", "p2", "Person", "k1", "knows", Direction.FORWARD));
        flattenAndExtendToP3.setPrev(flattenAndExtendToP2);
        flattenAndExtendToP2.setNext(flattenAndExtendToP3);

        var cidVar = new PropertyVariable(new NodeVariable("p1", personKey), "cid", DataType.INT);
        var cidVarEqualsTo = new ComparisonExpression(ComparisonOperator.LESS_THAN, cidVar,
            new IntLiteral(val)); // 22468883
        var cidFilter = new Filter(cidVarEqualsTo);
        flattenAndExtendToP2.setPrev(cidFilter);
        cidFilter.setNext(flattenAndExtendToP2);

        var reader = LDBC_PLANS.makeNodePropReaderWithOffset(graph, "p1", "Person", DataType.INT,
            "cid");
        cidFilter.setPrev(reader);
        reader.setNext(cidFilter);

        var scanP1 = new ScanAllNodes(new NodeVariable("p1", catalog.getTypeKey("Person")));
        reader.setPrev(scanP1);
        scanP1.setNext(reader);

        return sink;
    }
}
