package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataChunks {

    private final Schema schema = new Schema();
    private final List<DataChunk> dataChunks = new ArrayList<>();
    private final Map<Integer, Boolean> dataChunkPosToIsFlatMap = new HashMap<>() {};

    public DataChunk getDataChunk(String variable) {
        return dataChunks.get(schema.getDataChunkPos(variable));
    }

    public void reset() {
        for (var dataChunk : dataChunks) {
            dataChunk.resetSelector();
        }
    }

    public int getDataChunkPos(String variable) {
        return schema.getDataChunkPos(variable);
    }

    public DataChunk getLastExtendedToDataChunk(Set<String> variableNames) {
        DataChunk dataChunk = null;
        var lastExtendedToDataChunkPos = -1;
        for (var variableName : variableNames) {
            var dataChunkPos = schema.getDataChunkPos(variableName);
            if (lastExtendedToDataChunkPos < dataChunkPos) {
                dataChunk = dataChunks.get(dataChunkPos);
                lastExtendedToDataChunkPos = dataChunkPos;
            }
        }
        return dataChunk;
    }

    public ValueVector getValueVector(String variable) {
        return dataChunks.get(schema.getDataChunkPos(variable)).
            getValueVector(schema.getVectorPos(variable));
    }

    public void addVartoPos(String varName, int dataChunkPos, int vectorPos) {
        schema.addVartoPos(varName, dataChunkPos, vectorPos);
    }

    public DataChunk[] getUnflattenedDataChunks() {
        var count = 0;
        for (var pos : dataChunkPosToIsFlatMap.keySet()) {
            if (!dataChunkPosToIsFlatMap.get(pos)) {
                count++;
            }
        }
        var i = 0;
        var dataChunksAsLists = new DataChunk[count];
        for (var pos : dataChunkPosToIsFlatMap.keySet()) {
            if (!dataChunkPosToIsFlatMap.get(pos)) {
                count++;
                dataChunksAsLists[i++] = dataChunks.get(pos);
            }
        }
        return dataChunksAsLists;
    }

    public List<DataChunk> getDataChunkList() {
        return dataChunks;
    }

    public void insert(int dataChunkPos, ValueVector vector) {
        dataChunks.get(dataChunkPos).append(vector);
    }

    public void append(DataChunk dataChunk) {
        dataChunkPosToIsFlatMap.put(dataChunks.size(), false);
        dataChunks.add(dataChunk);
    }

    public boolean isFlat(String variable) {
        return dataChunkPosToIsFlatMap.get(schema.getDataChunkPos(variable));
    }

    public void setAsFlat(String variable) {
        dataChunkPosToIsFlatMap.put(schema.getDataChunkPos(variable), true);
    }

    public int size() {
        return dataChunks.size();
    }
}
