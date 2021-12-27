package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import ca.waterloo.dsg.graphflow.storage.BucketOffsetManager;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.GraphCatalog;
import ca.waterloo.dsg.graphflow.storage.adjlistindex.AdjListIndexes;
import ca.waterloo.dsg.graphflow.storage.properties.nodepropertystore.NodePropertyStore;
import ca.waterloo.dsg.graphflow.storage.properties.relpropertystore.RelPropertyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class QueryRunner extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(QueryRunner.class);

    protected static Graph graph;
    public static int NUM_WARMUP_RUNS = 2;
    public static int NUM_ACTUAL_RUNS = 3;

    public static void main(String[] args) throws IOException, ClassNotFoundException,
        InterruptedException {
        var cmdLine = parseCommandLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            return;
        }
        loadDataset(sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.INPUT_DIR)));
        NUM_WARMUP_RUNS = Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.WARM_UP_RUNS));
        NUM_ACTUAL_RUNS = Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.NUM_RUNS));
        var q = cmdLine.getOptionValue(ArgsFactory.QUERY_INDEX);
        switch (q) {
            case "H1_COUNT":
                executeAQuery(QUERY_PLANS.H1_COUNT(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H1_FILTER":
                executeAQuery(QUERY_PLANS.H1_FILTER(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H2_COUNT":
                executeAQuery(QUERY_PLANS.H2_COUNT(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H2_FILTER":
                executeAQuery(QUERY_PLANS.H2_FILTER(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H3_COUNT":
                executeAQuery(QUERY_PLANS.H3_COUNT(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H3_FILTER":
                executeAQuery(QUERY_PLANS.H3_FILTER(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "MICROP-01":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234254));
                break;
            case "MICROP-02":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234274));
                break;
            case "MICROP-03":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234314));
                break;
            case "MICROP-04":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234384));
                break;
            case "MICROP-05":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234504));
                break;
            case "MICROP-06":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82234874));
                break;
            case "MICROP-07":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82236234));
                break;
            case "MICROP-08":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82238034));
                break;
            case "MICROP-09":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82243534));
                break;
            case "MICROP-10":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82252934));
                break;
            case "MICROP-11":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82271834));
                break;
            case "MICROP-12":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82289934));
                break;
            case "MICROP-13":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82308434));
                break;
            case "MICROP-14":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82326934));
                break;
            case "MICROP-15":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82345034));
                break;
            case "MICROP-16":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82363334));
                break;
            case "MICROP-17":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82381534));
                break;
            case "MICROP-18":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82399734));
                break;
            case "MICROP-19":
                executeAQuery(LDBC_PLANS.MICRO08(graph, 82418234));
                break;
            case "MICROK-01":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1263736391));
                break;
            case "MICROK-02":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1264096391));
                break;
            case "MICROK-03":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1264736391));
                break;
            case "MICROK-04":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1265366391));
                break;
            case "MICROK-05":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1266216391));
                break;
            case "MICROK-06":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1267916391));
                break;
            case "MICROK-07":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1270216391));
                break;
            case "MICROK-08":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1273536391));
                break;
            case "MICROK-09":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1280516391));
                break;
            case "MICROK-10":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1288866391));
                break;
            case "MICROK-11":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1301606391));
                break;
            case "MICROK-12":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1311766391));
                break;
            case "MICROK-13":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1320486391));
                break;
            case "MICROK-14":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1328406391));
                break;
            case "MICROK-15":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1335216391));
                break;
            case "MICROK-16":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1341616391));
                break;
            case "MICROK-17":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1347496391));
                break;
            case "MICROK-18":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1353146391));
                break;
            case "MICROK-19":
                executeAQuery(LDBC_PLANS.MICRO09(graph, 1400000000));
                break;
            case "IC01A":
                executeAQuery(LDBC_PLANS.IC01A(graph));
                break;
            case "IC01B":
                executeAQuery(LDBC_PLANS.IC01B(graph));
                break;
            case "IC01C":
                executeAQuery(LDBC_PLANS.IC01(graph));
                break;
            case "IC02":
                executeAQuery(LDBC_PLANS.IC02(graph));
                break;
            case "IC03A":
                executeAQuery(LDBC_PLANS.IC03A(graph));
                break;
            case "IC03B":
                executeAQuery(LDBC_PLANS.IC03(graph));
                break;
            case "IC04":
                executeAQuery(LDBC_PLANS.IC04(graph));
                break;
            case "IC05A":
                executeAQuery(LDBC_PLANS.IC05A(graph));
                break;
            case "IC05B":
                executeAQuery(LDBC_PLANS.IC05(graph));
                break;
            case "IC06A":
                executeAQuery(LDBC_PLANS.IC06A(graph));
                break;
            case "IC06B":
                executeAQuery(LDBC_PLANS.IC06(graph));
                break;
            case "IC07":
                executeAQuery(LDBC_PLANS.IC07(graph));
                break;
            case "IC08":
                executeAQuery(LDBC_PLANS.IC08(graph));
                break;
            case "IC09A":
                executeAQuery(LDBC_PLANS.IC09A(graph));
                break;
            case "IC09B":
                executeAQuery(LDBC_PLANS.IC09(graph));
                break;
            case "IC11A":
                executeAQuery(LDBC_PLANS.IC11A(graph));
                break;
            case "IC11B":
                executeAQuery(LDBC_PLANS.IC11(graph));
                break;
            case "IC12":
                executeAQuery(LDBC_PLANS.IC12(graph));
                break;
            case "IS01":
                executeAQuery(LDBC_PLANS.IS01(graph));
                break;
            case "IS02":
                executeAQuery(LDBC_PLANS.IS02(graph));
                break;
            case "IS03":
                executeAQuery(LDBC_PLANS.IS03(graph));
                break;
            case "IS04":
                executeAQuery(LDBC_PLANS.IS04(graph));
                break;
            case "IS05":
                executeAQuery(LDBC_PLANS.IS05(graph));
                break;
            case "IS06":
                executeAQuery(LDBC_PLANS.IS06(graph));
                break;
            case "IS07":
                executeAQuery(LDBC_PLANS.IS07(graph));
                break;
            case "H1_F":
                executeAQuery(LDBC_PLANS.H1_F(graph));
                break;
            case "H1_B":
                executeAQuery(LDBC_PLANS.H1_B(graph));
                break;
            case "H2_F":
                executeAQuery(LDBC_PLANS.H2_F(graph));
                break;
            case "H2_B":
                executeAQuery(LDBC_PLANS.H2_B(graph));
                break;
            case "H3_F":
                executeAQuery(LDBC_PLANS.H3_F(graph));
                break;
            case "H3_B":
                executeAQuery(LDBC_PLANS.H3_B(graph));
                break;
            case "H2_F_WIKI":
                executeAQuery(QUERY_PLANS.H2_F_WIKI(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H2_B_WIKI":
                executeAQuery(QUERY_PLANS.H2_B_WIKI(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H3_F_WIKI":
                executeAQuery(QUERY_PLANS.H3_F_WIKI(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H3_B_WIKI":
                executeAQuery(QUERY_PLANS.H3_B_WIKI(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H1_FILTER_P":
                executeAQuery(QUERY_PLANS.H1_FILTER_P(graph,
                    Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.CID))));
                break;
            case "H2_FILTER_P":
                executeAQuery(QUERY_PLANS.H2_FILTER_P(graph));
                break;
        }
    }

    public static void loadDataset(String dir) throws IOException,
        ClassNotFoundException, InterruptedException {
        graph = new Graph();
        triggerGC();
        graph.setNodePropertyStore(NodePropertyStore.deserialize(dir));
        graph.setRelPropertyStore(RelPropertyStore.deserialize(dir));
        triggerGC();
        var indexes = new AdjListIndexes();
        indexes.setFwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
            Direction.FORWARD + "DefaultAdjListIndexes"));
        indexes.setFwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
            Direction.FORWARD + "ColumnAdjListIndexes"));
        triggerGC();
        indexes.setBwdDefaultAdjListIndexes(AdjListIndexes.deserializeDefaultAdjListIndexes(dir,
            Direction.BACKWARD + "DefaultAdjListIndexes"));
        indexes.setBwdColumnAdjListIndexes(AdjListIndexes.deserializeColumnAdjListIndexes(dir,
            Direction.BACKWARD + "ColumnAdjListIndexes"));
        triggerGC();
        graph.setAdjListIndexes(indexes);
        graph.setNumNodes((long) IOUtils.deserializeObject(dir + "numNodes"));
        graph.setNumNodesPerType((long[]) IOUtils.deserializeObject(dir + "numNodesPerType"));
        graph.setNumRels((long) IOUtils.deserializeObject(dir + "numRels"));
        graph.setNumRelsPerLabel((long[]) IOUtils.deserializeObject(dir + "numRelsPerLabel"));
        graph.setBucketOffsetManagers((BucketOffsetManager[][]) IOUtils.deserializeObject(
            dir + "bucketOffsetManagers"));
        graph.setGraphCatalog((GraphCatalog) IOUtils.deserializeObject(dir + "graphCatalog"));
        triggerGC();
        // indexStore = IOUtils.deserializeIndexStore(dir);
    }

    private static void triggerGC() throws InterruptedException {
        System.gc();
        Thread.sleep(1000);
    }

    public static void executeAQuery(Operator lastOperator) {
        lastOperator.init(graph);
        for (var i = 0; i < NUM_WARMUP_RUNS; i++) {
            lastOperator.execute();
            logger.info(String.format("\tNumber output tuples: %d", lastOperator.getNumOutTuples()));
            lastOperator.reset();
        }
        var runtime = 0.0d;
        for (var i = 0; i < NUM_ACTUAL_RUNS; i++) {
            var startTime = System.nanoTime();
            lastOperator.execute();
            var elapsed_time = IOUtils.getTimeDiff(startTime);
            runtime += elapsed_time;
            logger.info(String.format("\tNumber output tuples: %d", lastOperator.getNumOutTuples()));
            lastOperator.reset();
        }
        logger.info(String.format("\tElapsed time (ms): %.3f", (runtime / (double)NUM_ACTUAL_RUNS));
    }

    private static Options getCommandLineOptions() {
        var options = new Options();
        options.addOption(ArgsFactory.getInputGraphDirectoryOption());    // INPUT_DIR          -i
        options.addOption(ArgsFactory.getQueryIndex());                   // QUERY_INDEX        -q
        options.addOption(ArgsFactory.getWarmupRunsOption());             // WARM_UP            -w
        options.addOption(ArgsFactory.getNumberRunsOptions());            // NUM_RUNS           -r
        options.addOption(ArgsFactory.getCID());                          // CID                -c
        return options;
    }
}
