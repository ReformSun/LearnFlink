package model;

import org.apache.flink.api.java.tuple.Tuple;

public class TestCSV extends Tuple{




    @Override
    public <T> T getField(int i) {
        return null;
    }

    @Override
    public <T> void setField(T t, int i) {

    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public <T extends Tuple> T copy() {
        return null;
    }
}
