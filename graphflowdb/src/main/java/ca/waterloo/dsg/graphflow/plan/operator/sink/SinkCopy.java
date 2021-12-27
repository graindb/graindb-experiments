package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.datachunk.DataChunk;
import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.storage.Graph;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SinkCopy extends Operator implements Serializable {

    final static int BUFFER_SIZE = 1000000; // 1M.

    private int[] intBuffer = new int[BUFFER_SIZE];
    private int[] stringBuffer = new int[BUFFER_SIZE];

    private static class vectorsWrapped {
        ValueVector[] vectorsFlattened;
        DataChunk[] dataChunksFlattened;
        ValueVector[] vectorsAsLists;
        public DataChunk[] dataChunksContainingLists;
    }

    String[] intVectorsFlattenedName, intVectorsAsListsName;
    String[] strVectorsFlattenedName, strVectorsAsListsName;
    // String[] doubleVectorsFlattenedName, doubleVectorsAsListsName;

    // 0 -> int, 1 -> double, 2 -> string.
    vectorsWrapped[] vectorWrappers;

    int[][] intValues;
    String[][] stringValues;
    // double[][] doubleValues;
    int numTuples;

    List<DataChunk> dataChunkList;

    public SinkCopy(String[] intVectorsFlattenedName, String[] intVectorsAsListsName,
        // String[] doubleVectorsFlattenedName, String[] doubleVectorsAsListsName,
        String[] strVectorsFlattenedName, String[] strVectorsAsListsName) {
        this.intVectorsFlattenedName = intVectorsFlattenedName;
        this.intVectorsAsListsName = intVectorsAsListsName;
        this.strVectorsFlattenedName = strVectorsFlattenedName;
        this.strVectorsAsListsName = strVectorsAsListsName;
        // this.doubleVectorsFlattenedName = doubleVectorsFlattenedName;
        // this.doubleVectorsAsListsName = doubleVectorsAsListsName;
    }

    @Override
    protected void initFurther(Graph graph) {
        // 0 -> int, 1 -> string, 2 -> double.
        vectorWrappers = new vectorsWrapped[2];
        vectorWrappers[0] = new vectorsWrapped();
        vectorWrappers[1] = new vectorsWrapped();
        loadVectorsAndDataChunks(vectorWrappers[0], intVectorsFlattenedName, intVectorsAsListsName);
        loadVectorsAndDataChunks(vectorWrappers[1], strVectorsFlattenedName, strVectorsAsListsName);
        intValues = new int[intVectorsFlattenedName.length + intVectorsAsListsName.length][BUFFER_SIZE];
        stringValues = new String[strVectorsFlattenedName.length + strVectorsAsListsName.length][BUFFER_SIZE];
        //loadVectorsAndDataChunks(vectorWrappers[2], doubleVectorsFlattenedName,doubleVectorsAsListsName);
        //doubleValues = new double[doubleVectorsFlattenedName.length + doubleVectorsAsListsName.length][BUFFER_SIZE];
    }

    @Override
    public void reset() {
        prev.getDataChunks().reset();
        numOutTuples = 0;
        prev.reset();
    }

    private void loadVectorsAndDataChunks(vectorsWrapped vectorWrapper,
        String[] vectorsFlattenedName, String[] vectorsAsListsName) {
        vectorWrapper.vectorsFlattened = new ValueVector[vectorsFlattenedName.length];
        vectorWrapper.dataChunksFlattened = new DataChunk[vectorsFlattenedName.length];
        vectorWrapper.vectorsAsLists = new ValueVector[vectorsAsListsName.length];
        vectorWrapper.dataChunksContainingLists = new DataChunk[vectorsAsListsName.length];
        var i = 0;
        for (var name : vectorsFlattenedName) {
            vectorWrapper.vectorsFlattened[i] = dataChunks.getValueVector(name);
            vectorWrapper.dataChunksFlattened[i++] = dataChunks.getDataChunk(name);
        }
        i = 0;
        for (var name : vectorsAsListsName) {
            vectorWrapper.vectorsAsLists[i] = dataChunks.getValueVector(name);
            vectorWrapper.dataChunksContainingLists[i++] = dataChunks.getDataChunk(name);
        }
    }

    @Override
    public void processNewDataChunks() {
        numTuples = 1;
        for (var dataChunk : dataChunks.getDataChunkList()) {
            if (dataChunk.currentPos == -1) {
                numTuples *= dataChunk.getNumTrueValuesInSelector();
            }
        }
        var numTuplesToCopy = numTuples;
        copyFlatInts(numTuplesToCopy);
        copyFlatStrings(numTuplesToCopy);
        var nextNumReptitions = copyListInts(numTuplesToCopy, 1 /* numRepetitions */);
        copyListStrings(numTuplesToCopy, nextNumReptitions);
        numOutTuples += numTuplesToCopy;
    }

    private void copyFlatInts(int numTuplesToCopy) {
        for (var i = 0; i < vectorWrappers[0].vectorsFlattened.length; i++) {
            Arrays.fill(intValues[i],
                0 /* from_Index */, numTuplesToCopy /* to_Index */,
                vectorWrappers[0].vectorsFlattened[i].getInt(
                    vectorWrappers[0].dataChunksFlattened[i].currentPos
                )
            );
        }
    }

    private int copyListInts(int numTuplesToCopy, int numElementRepetitions) {
        var numFlatInts = vectorWrappers[0].vectorsFlattened.length;
        for (var i = 0; i < vectorWrappers[0].vectorsAsLists.length; i++) {
            var intArr = intValues[i + numFlatInts];
            var vector = vectorWrappers[0].vectorsAsLists[i];
            var dataChunk = vectorWrappers[0].dataChunksContainingLists[i];
            var selector = dataChunk.getSelector();
            var writePos = 0;
            var numCopies = numTuplesToCopy / dataChunk.getNumTrueValuesInSelector();
            for (var j = 0; j < numCopies; j++) {
                for (var k = 0; k < dataChunk.size(); k++) {
                    if (selector[k]) {
                        Arrays.fill(intArr, writePos, writePos + numElementRepetitions, vector.getInt(k));
                        writePos += numElementRepetitions;
                    }
                }
            }
            if (i > 0 && dataChunk != vectorWrappers[0].dataChunksContainingLists[i - 1]) {
                numElementRepetitions *= dataChunk.getNumTrueValuesInSelector();
            }
        }
        return numElementRepetitions;
    }

    private void copyFlatStrings(int numTuplesToCopy) {
        for (var i = 0; i < vectorWrappers[1].vectorsFlattened.length; i++) {
            Arrays.fill(stringValues[i],
                0 /* from_Index */, numTuplesToCopy /* to_Index */,
                vectorWrappers[1].vectorsFlattened[i].getString(
                    vectorWrappers[1].dataChunksFlattened[i].currentPos
                )
            );
        }
    }

    private int copyListStrings(int numTuplesToCopy, int numElementRepetitions) {
        var numFlatStrs = vectorWrappers[1].vectorsFlattened.length;
        for (var i = 0; i < vectorWrappers[1].vectorsAsLists.length; i++) {
            var strArr = stringValues[i + numFlatStrs];
            var vector = vectorWrappers[1].vectorsAsLists[i];
            var dataChunk = vectorWrappers[1].dataChunksContainingLists[i];
            var selector = dataChunk.getSelector();
            var writePos = 0;
            var numCopies = numTuplesToCopy / dataChunk.getNumTrueValuesInSelector();
            for (var j = 0; j < numCopies; j++) {
                for (var k = 0; k < dataChunk.size(); k++) {
                    if (selector[k]) {
                        Arrays.fill(strArr, writePos, writePos + numElementRepetitions, vector.getString(k));
                        writePos += numElementRepetitions;
                    }
                }
            }
            if (i > 0 && dataChunk != vectorWrappers[1].dataChunksContainingLists[i - 1]) {
                numElementRepetitions *= dataChunk.getNumTrueValuesInSelector();
            }
        }
        return numElementRepetitions;
    }
}
