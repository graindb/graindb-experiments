package ca.waterloo.dsg.graphflow.datachunk;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataChunk {

    List<ValueVector> vectors;
    @Setter @Getter boolean[] selector;
    public int currentPos = -1;
    @Getter int numTrueValuesInSelector;
    int size;

    public DataChunk(int maxSizePossible) {
        selector = new boolean[maxSizePossible];
        Arrays.fill(selector, true);
        resetSelector();
        vectors = new ArrayList<>();
    }

    public ValueVector getValueVector(int pos) {
        return vectors.get(pos);
    }

    public int getNumValueVectors() {
        return vectors.size();
    }

    public void append(ValueVector vector) {
        vectors.add(vector);
    }

    public void setSizeAndNumTrueValuesInSelector(int size) {
        this.size = size;
        this.numTrueValuesInSelector = size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void resetSelector() {
        Arrays.fill(selector, 0, size, true);
    }

    public void setNumTrueValuesInSelector(int numTrueValuesInSelector) {
        this.numTrueValuesInSelector = numTrueValuesInSelector;
    }

    public int size() {
        return size;
    }
}
