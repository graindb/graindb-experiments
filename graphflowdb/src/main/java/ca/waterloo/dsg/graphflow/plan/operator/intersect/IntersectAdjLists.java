package ca.waterloo.dsg.graphflow.plan.operator.intersect;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;

import java.util.List;

public abstract class IntersectAdjLists extends Operator {

    List<AdjListDescriptor> ALDs;

    public IntersectAdjLists(List<AdjListDescriptor> ALDs) {
        this.ALDs = ALDs;
        // TODO: sort ALDs.
    }
}
