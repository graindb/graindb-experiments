package ca.waterloo.dsg.graphflow.parser.query.expressions.literals;

import ca.waterloo.dsg.graphflow.datachunk.vectors.ValueVector;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import lombok.Getter;

import java.util.Objects;

public class IntLiteral extends LiteralTerm {

    @Getter private int intLiteral;

    public IntLiteral(int intLiteral) {
        super(intLiteral + "", DataType.INT);
        this.intLiteral = intLiteral;
        values = ValueVector.make(DataType.INT, 1);
        values.set(0, intLiteral);
    }

    public void setNewLiteralValue(int val) {
        intLiteral = val;
        values.set(0, intLiteral);
    }

    @Override
    public Object getLiteral() {
        return intLiteral;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*31 + Objects.hash(intLiteral);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        var otherLongLiteral = (IntLiteral) o;
        return this.intLiteral == otherLongLiteral.intLiteral &&
            this.values.getInt(0) == otherLongLiteral.values.getInt(0);
    }
}
