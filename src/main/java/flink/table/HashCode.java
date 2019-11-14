package flink.table;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCode extends ScalarFunction {
    int factor = 12;
    public HashCode(int factor) {
        this.factor = factor;
    }

    public int eval(String str) {
        return str.hashCode() * factor;
    }
}
