package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.util.container.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Schema of a tuple or table, i.e., an unordered set of (variable variableName, {@link DataChunk)
 * pairs.
 */
public class Schema implements Serializable {

    private static final Logger logger = LogManager.getLogger(Schema.class);

    Map<String, Pair<Integer /* dataChunk pos */, Integer /* vector pos */>> varNameToTuplePos =
        new HashMap<>();

    public void addVartoPos(String varName, int dataChunkPos, int vectorPos) {
        varNameToTuplePos.put(varName, new Pair<>(dataChunkPos, vectorPos));
    }

    public int getDataChunkPos(String varName) {
        return varNameToTuplePos.get(varName).a;
    }

    public int getVectorPos(String varName) {
        return varNameToTuplePos.get(varName).b;
    }
}
