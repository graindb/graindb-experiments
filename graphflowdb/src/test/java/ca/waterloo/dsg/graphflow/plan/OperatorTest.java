package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.parser.query.expressions.ComparisonExpression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.PropertyVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.literals.IntLiteral;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.onlyextend.ExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.adjlists.flattenandextend.FlattenAndExtendAdjLists;
import ca.waterloo.dsg.graphflow.plan.operator.extend.column.ExtendColumn;
import ca.waterloo.dsg.graphflow.plan.operator.filter.Filter;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.PropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node.NodePropertyReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.node.NodePropertyWithOffsetReader;
import ca.waterloo.dsg.graphflow.plan.operator.propertyreader.rel.RelPropertyReader.RelPropertyIntReader;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanAllNodes;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanRangeNodes;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanSingleNode;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkCount;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.util.DataLoader;
import ca.waterloo.dsg.graphflow.util.datatype.ComparisonOperator;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OperatorTest {

    static Graph graphTinySnb;

    /* expected property values for PERSON node. */
    String[] expectedFName = { "Alice", "Bob", "Carol", "Dan", "Elizabeth", "Farooq", "Greg" };
    boolean[] expectedIsStudent = { true, true, false, false, false, true, false };
    double[] expectedEyeSight = { 5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9 };
    boolean[] expectedIsWorker = { false, false, true, true, true, false, false };
    int[] expectedGender = { 1, 2, 1, 2, 1, 2, 2 };
    int[] expectedAge = { 35, 30, 45, 20, 20, 25, 40 };

    int[] expectedDate = { 1234567890, 1234567890, 1234567890, 1234567890, 1234567892, 1234567892,
        1234567890, 1234567892, 1234567893, 1234567890, 1234567892, 1234567893, 1234567897,
        1234567897
    };

    @BeforeAll
    public static void setUp() {
        graphTinySnb = DataLoader.getDataset("tiny-snb").graph;
    }

    @Test
    public void testScanPerson() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();
        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        sink.setPrev(scan);
        scan.setNext(sink);
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(7, sink.getNumOutTuples());
    }

    @Test
    public void testScanAllNodesPersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var readerAge = makeNodePropReader("a", "PERSON", DataType.INT, "age");
        sink.setPrev(readerAge);
        readerAge.setNext(sink);

        var readerGender = makeNodePropReader("a", "PERSON", DataType.INT, "gender");
        readerAge.setPrev(readerGender);
        readerGender.setNext(readerAge);

        var readerIsWorker = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isWorker");
        readerGender.setPrev(readerIsWorker);
        readerIsWorker.setNext(readerGender);

        var readerEyeSight = makeNodePropReader("a", "PERSON", DataType.DOUBLE, "eyeSight");
        readerIsWorker.setPrev(readerEyeSight);
        readerEyeSight.setNext(readerIsWorker);

        var readerIsStudent = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isStudent");
        readerEyeSight.setPrev(readerIsStudent);
        readerIsStudent.setNext(readerEyeSight);

        var readerFName = makeNodePropReader("a", "PERSON", DataType.STRING, "fName");
        readerIsStudent.setPrev(readerFName);
        readerFName.setNext(readerIsStudent);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        readerFName.setPrev(scan);
        scan.setNext(readerFName);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorFName = dataChunks.getValueVector("a.fName");
        var vectorIsStudent = dataChunks.getValueVector("a.isStudent");
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorIsWorker = dataChunks.getValueVector("a.isWorker");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        for (var i = 0; i < expectedFName.length; i++) {
            Assertions.assertEquals(expectedFName[i], vectorFName.getString(i));
            Assertions.assertEquals(expectedIsStudent[i], vectorIsStudent.getBoolean(i));
            Assertions.assertEquals(expectedEyeSight[i], vectorEyeSight.getDouble(i));
            Assertions.assertEquals(expectedIsWorker[i], vectorIsWorker.getBoolean(i));
            Assertions.assertEquals(expectedGender[i], vectorGender.getInt(i));
            Assertions.assertEquals(expectedAge[i], vectorAge.getInt(i));
        }
        Assertions.assertEquals(7, sink.getNumOutTuples());
    }

    @Test
    public void testScanAndFilters() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var key = graphTinySnb.getGraphCatalog().getTypeKey("PERSON");
        var agePropVar = new PropertyVariable(new NodeVariable("a", key), "age");
        agePropVar.setDataType(DataType.INT);
        var ageGreaterOrEqualTo30 = new ComparisonExpression(ComparisonOperator.
            GREATER_THAN_OR_EQUAL, agePropVar, new IntLiteral(30));
        var filter = new Filter(ageGreaterOrEqualTo30);
        sink.setPrev(filter);
        filter.setNext(sink);

        var readerAge = makeNodePropReader("a", "PERSON", DataType.INT, "age");
        filter.setPrev(readerAge);
        readerAge.setNext(filter);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        readerAge.setPrev(scan);
        scan.setNext(readerAge);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var selector = dataChunks.getDataChunk("a.age").getSelector();
        boolean[] expectedSelectorVals = { true, true, true, false, false, false, true };
        for (var i = 0; i < expectedFName.length; i++) {
            Assertions.assertEquals(expectedSelectorVals[i], selector[i]);
        }
        Assertions.assertEquals(4, sink.getNumOutTuples());
    }

    @Test
    public void testScanSingleNodePersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var readerAge = makeNodePropReader("a", "PERSON", DataType.INT, "age");
        sink.setPrev(readerAge);
        readerAge.setNext(sink);

        var readerGender = makeNodePropReader("a", "PERSON", DataType.INT, "gender");
        readerAge.setPrev(readerGender);
        readerGender.setNext(readerAge);

        var readerIsWorker = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isWorker");
        readerGender.setPrev(readerIsWorker);
        readerIsWorker.setNext(readerGender);

        var readerEyeSight = makeNodePropReader("a", "PERSON", DataType.DOUBLE, "eyeSight");
        readerIsWorker.setPrev(readerEyeSight);
        readerEyeSight.setNext(readerIsWorker);

        var readerIsStudent = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isStudent");
        readerEyeSight.setPrev(readerIsStudent);
        readerIsStudent.setNext(readerEyeSight);

        var readerFName = makeNodePropReader("a", "PERSON", DataType.STRING, "fName");
        readerIsStudent.setPrev(readerFName);
        readerFName.setNext(readerIsStudent);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 2);
        readerFName.setPrev(scan);
        scan.setNext(readerFName);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorFName = dataChunks.getValueVector("a.fName");
        var vectorIsStudent = dataChunks.getValueVector("a.isStudent");
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorIsWorker = dataChunks.getValueVector("a.isWorker");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        Assertions.assertEquals(expectedFName[2], vectorFName.getString(0));
        Assertions.assertEquals(expectedIsStudent[2], vectorIsStudent.getBoolean(0));
        Assertions.assertEquals(expectedEyeSight[2], vectorEyeSight.getDouble(0));
        Assertions.assertEquals(expectedIsWorker[2], vectorIsWorker.getBoolean(0));
        Assertions.assertEquals(expectedGender[2], vectorGender.getInt(0));
        Assertions.assertEquals(expectedAge[2], vectorAge.getInt(0));
        Assertions.assertEquals(1, sink.getNumOutTuples());
    }

    @Test
    public void testScanRangeNodePersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var readerAge = makeNodePropReader("a", "PERSON", DataType.INT, "age");
        sink.setPrev(readerAge);
        readerAge.setNext(sink);

        var readerGender = makeNodePropReader("a", "PERSON", DataType.INT, "gender");
        readerAge.setPrev(readerGender);
        readerGender.setNext(readerAge);

        var readerIsWorker = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isWorker");
        readerGender.setPrev(readerIsWorker);
        readerIsWorker.setNext(readerGender);

        var readerEyeSight = makeNodePropReader("a", "PERSON", DataType.DOUBLE, "eyeSight");
        readerIsWorker.setPrev(readerEyeSight);
        readerEyeSight.setNext(readerIsWorker);

        var readerIsStudent = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isStudent");
        readerEyeSight.setPrev(readerIsStudent);
        readerIsStudent.setNext(readerEyeSight);

        var readerFName = makeNodePropReader("a", "PERSON", DataType.STRING, "fName");
        readerIsStudent.setPrev(readerFName);
        readerFName.setNext(readerIsStudent);

        var scan = new ScanRangeNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")),
            1, 5, 1);
        readerFName.setPrev(scan);
        scan.setNext(readerFName);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorFName = dataChunks.getValueVector("a.fName");
        var vectorIsStudent = dataChunks.getValueVector("a.isStudent");
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorIsWorker = dataChunks.getValueVector("a.isWorker");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        for (var i = 0; i < 4; i++) {
            Assertions.assertEquals(expectedFName[i + 1], vectorFName.getString(i));
            Assertions.assertEquals(expectedIsStudent[i + 1], vectorIsStudent.getBoolean(i));
            Assertions.assertEquals(expectedEyeSight[i + 1], vectorEyeSight.getDouble(i));
            Assertions.assertEquals(expectedIsWorker[i + 1], vectorIsWorker.getBoolean(i));
            Assertions.assertEquals(expectedGender[i + 1], vectorGender.getInt(i));
            Assertions.assertEquals(expectedAge[i + 1], vectorAge.getInt(i));
        }
        Assertions.assertEquals(4, sink.getNumOutTuples());
    }

    @Test
    public void testScanRangeNode2PersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var readerAge = makeNodePropReader("a", "PERSON", DataType.INT, "age");
        sink.setPrev(readerAge);
        readerAge.setNext(sink);

        var readerGender = makeNodePropReader("a", "PERSON", DataType.INT, "gender");
        readerAge.setPrev(readerGender);
        readerGender.setNext(readerAge);

        var readerIsWorker = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isWorker");
        readerGender.setPrev(readerIsWorker);
        readerIsWorker.setNext(readerGender);

        var readerEyeSight = makeNodePropReader("a", "PERSON", DataType.DOUBLE, "eyeSight");
        readerIsWorker.setPrev(readerEyeSight);
        readerEyeSight.setNext(readerIsWorker);

        var readerIsStudent = makeNodePropReader("a", "PERSON", DataType.BOOLEAN, "isStudent");
        readerEyeSight.setPrev(readerIsStudent);
        readerIsStudent.setNext(readerEyeSight);

        var readerFName = makeNodePropReader("a", "PERSON", DataType.STRING, "fName");
        readerIsStudent.setPrev(readerFName);
        readerFName.setNext(readerIsStudent);

        var scan = new ScanRangeNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")),
            1, 5, 2);
        readerFName.setPrev(scan);
        scan.setNext(readerFName);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorFName = dataChunks.getValueVector("a.fName");
        var vectorIsStudent = dataChunks.getValueVector("a.isStudent");
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorIsWorker = dataChunks.getValueVector("a.isWorker");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        for (var i = 0; i < 2; i++) {
            Assertions.assertEquals(expectedFName[i * 2 + 1], vectorFName.getString(i));
            Assertions.assertEquals(expectedIsStudent[i * 2 + 1], vectorIsStudent.getBoolean(i));
            Assertions.assertEquals(expectedEyeSight[i * 2 + 1], vectorEyeSight.getDouble(i));
            Assertions.assertEquals(expectedIsWorker[i * 2 + 1], vectorIsWorker.getBoolean(i));
            Assertions.assertEquals(expectedGender[i * 2 + 1], vectorGender.getInt(i));
            Assertions.assertEquals(expectedAge[i * 2 + 1], vectorAge.getInt(i));
        }
        Assertions.assertEquals(2, sink.getNumOutTuples());
    }

    @Test
    public void testScanRangeNode3PersonAndNodePropertyReaders() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();

        var readerAge = makeNodePropReaderWithOffset("a", "PERSON", DataType.INT, "age");
        sink.setPrev(readerAge);
        readerAge.setNext(sink);

        var readerGender = makeNodePropReaderWithOffset("a", "PERSON", DataType.INT, "gender");
        readerAge.setPrev(readerGender);
        readerGender.setNext(readerAge);

        var readerEyeSight = makeNodePropReaderWithOffset("a", "PERSON", DataType.DOUBLE, "eyeSight");
        readerGender.setPrev(readerEyeSight);
        readerEyeSight.setNext(readerGender);

        var scan = new ScanRangeNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")), 1, 5, 1);
        readerEyeSight.setPrev(scan);
        scan.setNext(readerEyeSight);

        sink.init(graphTinySnb);
        sink.execute();

        var dataChunks = sink.getDataChunks();
        var vectorEyeSight = dataChunks.getValueVector("a.eyeSight");
        var vectorGender = dataChunks.getValueVector("a.gender");
        var vectorAge = dataChunks.getValueVector("a.age");

        for (var i = 0; i < 4; i++) {
            Assertions.assertEquals(expectedEyeSight[i + 1], vectorEyeSight.getDouble(i));
            Assertions.assertEquals(expectedGender[i + 1], vectorGender.getInt(i));
            Assertions.assertEquals(expectedAge[i + 1], vectorAge.getInt(i));
        }
        Assertions.assertEquals(4, sink.getNumOutTuples());
    }

    @Test
    public void testScanOrganisation() {
        var catalog = graphTinySnb.getGraphCatalog();
        var sink = new SinkCount();
        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("ORGANISATION")));
        sink.setPrev(scan);
        scan.setNext(sink);
        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());
    }

    @Test
    public void test1hop() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e1", "KNOWS", Direction.FORWARD));
        sink.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(sink);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(14, sink.getNumOutTuples());
    }

    @Test
    public void test2hop() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var flattenAndExtend2 = new FlattenAndExtendAdjLists(makeALD(catalog, "b", "PERSON", "c",
            "PERSON", "e2", "KNOWS", Direction.FORWARD));
        sink.setPrev(flattenAndExtend2);
        flattenAndExtend2.setNext(sink);

        var flattenAndExtend1 = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e1", "KNOWS", Direction.FORWARD));
        flattenAndExtend2.setPrev(flattenAndExtend1);
        flattenAndExtend1.setNext(flattenAndExtend2);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        flattenAndExtend1.setPrev(scan);
        scan.setNext(flattenAndExtend1);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(36, sink.getNumOutTuples());
    }

    @Test
    public void test3hop() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var flattenAndExtend3 = new FlattenAndExtendAdjLists(makeALD(catalog, "c", "PERSON", "d",
            "PERSON", "e3", "KNOWS", Direction.FORWARD));
        sink.setPrev(flattenAndExtend3);
        flattenAndExtend3.setNext(sink);

        var flattenAndExtend2 = new FlattenAndExtendAdjLists(makeALD(catalog, "b", "PERSON", "c",
            "PERSON", "e2", "KNOWS", Direction.FORWARD));
        flattenAndExtend3.setPrev(flattenAndExtend2);
        flattenAndExtend2.setNext(flattenAndExtend3);

        var flattenAndExtend1 = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e1", "KNOWS", Direction.FORWARD));
        flattenAndExtend2.setPrev(flattenAndExtend1);
        flattenAndExtend1.setNext(flattenAndExtend2);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        flattenAndExtend1.setPrev(scan);
        scan.setNext(flattenAndExtend1);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(108, sink.getNumOutTuples());
    }

    @Test
    public void test3star() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var extend2 = new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "d",
            "PERSON", "e3", "KNOWS", Direction.FORWARD));
        sink.setPrev(extend2);
        extend2.setNext(sink);

        var extend1 = new ExtendAdjLists(makeALD(catalog, "a", "PERSON", "c",
            "PERSON", "e2", "KNOWS", Direction.FORWARD));
        extend2.setPrev(extend1);
        extend1.setNext(extend2);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e1", "KNOWS", Direction.FORWARD));
        extend1.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(extend1);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(116, sink.getNumOutTuples());
    }

    @Test
    public void testColExtend() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var extendColumn = new ExtendColumn(makeALD(catalog, "a", "PERSON", "b",
            "ORGANISATION", "e1", "STUDYAT", Direction.FORWARD));
        sink.setPrev(extendColumn);
        extendColumn.setNext(sink);

        var scan = new ScanAllNodes(new NodeVariable("a", catalog.getTypeKey("PERSON")));
        extendColumn.setPrev(scan);
        scan.setNext(extendColumn);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());
    }

    @Test
    public void testRelPropertyReaderAt0() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graphTinySnb.getGraphCatalog(), "a", "PERSON", "b", "PERSON", "e",
            "KNOWS", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var readerDate = RelPropertyIntReader.make(cDateVar, graphTinySnb.getRelPropertyStore(),
            graphTinySnb.getGraphCatalog());
        sink.setPrev(readerDate);
        readerDate.setNext(sink);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e", "KNOWS", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(readerDate);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 0);
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());

        var dataChunk = sink.getDataChunks().getDataChunk("e.date");
        var vectorDate = sink.getDataChunks().getValueVector("e.date");
        for (var i = 0; i < dataChunk.size(); i++) {
            Assertions.assertEquals(expectedDate[i], vectorDate.getInt(i));
        }
    }

    @Test
    public void testRelPropertyReaderAt1() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graphTinySnb.getGraphCatalog(), "a", "PERSON", "b", "PERSON", "e",
            "KNOWS", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var readerDate = RelPropertyIntReader.make(cDateVar, graphTinySnb.getRelPropertyStore(),
            graphTinySnb.getGraphCatalog());
        sink.setPrev(readerDate);
        readerDate.setNext(sink);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e", "KNOWS", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(readerDate);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 1);
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());

        var dataChunk = sink.getDataChunks().getDataChunk("e.date");
        var vectorDate = sink.getDataChunks().getValueVector("e.date");
        for (var i = 0; i < dataChunk.size(); i++) {
            Assertions.assertEquals(expectedDate[i + 3], vectorDate.getInt(i));
        }
    }

    @Test
    public void testRelPropertyReaderAt2() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graphTinySnb.getGraphCatalog(), "a", "PERSON", "b", "PERSON", "e",
            "KNOWS", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var readerDate = RelPropertyIntReader.make(cDateVar, graphTinySnb.getRelPropertyStore(),
            graphTinySnb.getGraphCatalog());
        sink.setPrev(readerDate);
        readerDate.setNext(sink);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e", "KNOWS", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(readerDate);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 2);
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());

        var dataChunk = sink.getDataChunks().getDataChunk("e.date");
        var vectorDate = sink.getDataChunks().getValueVector("e.date");
        for (var i = 0; i < dataChunk.size(); i++) {
            Assertions.assertEquals(expectedDate[i + 6], vectorDate.getInt(i));
        }
    }

    @Test
    public void testRelPropertyReaderAt3() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graphTinySnb.getGraphCatalog(), "a", "PERSON", "b", "PERSON", "e",
            "KNOWS", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var readerDate = RelPropertyIntReader.make(cDateVar, graphTinySnb.getRelPropertyStore(),
            graphTinySnb.getGraphCatalog());
        sink.setPrev(readerDate);
        readerDate.setNext(sink);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e", "KNOWS", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(readerDate);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 3);
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(3, sink.getNumOutTuples());

        var dataChunk = sink.getDataChunks().getDataChunk("e.date");
        var vectorDate = sink.getDataChunks().getValueVector("e.date");
        for (var i = 0; i < dataChunk.size(); i++) {
            Assertions.assertEquals(expectedDate[i + 9], vectorDate.getInt(i));
        }
    }

    @Test
    public void testRelPropertyReaderAt4() {
        var catalog = graphTinySnb.getGraphCatalog();

        var sink = new SinkCount();

        var relVar = makeRelVar(graphTinySnb.getGraphCatalog(), "a", "PERSON", "b", "PERSON", "e",
            "KNOWS", Direction.FORWARD);
        var cDateVar = new PropertyVariable(relVar, "date", DataType.INT);

        var readerDate = RelPropertyIntReader.make(cDateVar, graphTinySnb.getRelPropertyStore(),
            graphTinySnb.getGraphCatalog());
        sink.setPrev(readerDate);
        readerDate.setNext(sink);

        var flattenAndExtend = new FlattenAndExtendAdjLists(makeALD(catalog, "a", "PERSON", "b",
            "PERSON", "e", "KNOWS", Direction.FORWARD));
        readerDate.setPrev(flattenAndExtend);
        flattenAndExtend.setNext(readerDate);

        var scan = new ScanSingleNode(new NodeVariable("a", catalog.getTypeKey("PERSON")), 4);
        flattenAndExtend.setPrev(scan);
        scan.setNext(flattenAndExtend);

        sink.init(graphTinySnb);
        sink.execute();
        Assertions.assertEquals(2, sink.getNumOutTuples());

        var dataChunk = sink.getDataChunks().getDataChunk("e.date");
        var vectorDate = sink.getDataChunks().getValueVector("e.date");
        for (var i = 0; i < dataChunk.size(); i++) {
            Assertions.assertEquals(expectedDate[i + 12], vectorDate.getInt(i));
        }
    }

    public PropertyReader makeNodePropReader(String variable, String nodeType,
        DataType dataType, String property, boolean withOffset) {
        var typeKey = graphTinySnb.getGraphCatalog().getTypeKey(nodeType);
        var propVar = new PropertyVariable(new NodeVariable(variable, typeKey), property);
        propVar.setDataType(dataType);
        propVar.setPropertyKey(graphTinySnb.getGraphCatalog().getNodePropertyKey(property));
        return withOffset ?
            NodePropertyWithOffsetReader.make(propVar, graphTinySnb.getNodePropertyStore()) :
            NodePropertyReader.make(propVar, graphTinySnb.getNodePropertyStore());
    }

    public PropertyReader makeNodePropReader(String variable, String nodeType,
        DataType dataType, String property) {
        return makeNodePropReader(variable, nodeType, dataType, property, false);
    }

    public PropertyReader makeNodePropReaderWithOffset(String variable, String nodeType,
        DataType dataType, String property) {
        return makeNodePropReader(variable, nodeType, dataType, property, true);
    }

    private RelVariable makeRelVar(GraphCatalog catalog, String boundVarName, String boundVarLabel,
        String nbrVarName, String nbrVarLabel, String relVarName, String relVarLabel,
        Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        return direction == Direction.FORWARD ?
            new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar) :
            new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), nbrVar, boundVar);
    }

    public AdjListDescriptor makeALD(GraphCatalog catalog, String boundVarName,
        String boundVarLabel, String nbrVarName, String nbrVarLabel, String relVarName,
        String relVarLabel, Direction direction) {
        var boundVar = new NodeVariable(boundVarName, catalog.getTypeKey(boundVarLabel));
        var nbrVar = new NodeVariable(nbrVarName, catalog.getTypeKey(nbrVarLabel));
        var relVar = new RelVariable(relVarName, catalog.getLabelKey(relVarLabel), boundVar, nbrVar);
        return new AdjListDescriptor(relVar, boundVar, nbrVar, direction);
    }
}
