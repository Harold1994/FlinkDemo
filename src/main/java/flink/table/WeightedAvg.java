package flink.table;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;



public class WeightedAvg extends AggregateFunction<Long, WeightAvgAcc> {

    @Override
    public WeightAvgAcc createAccumulator() {
        return new WeightAvgAcc();
    }

    @Override
    public Long getValue(WeightAvgAcc weightAvgAcc) {
        if (weightAvgAcc.count == 0) {
            return null;
        } else {
            return weightAvgAcc.sum/weightAvgAcc.count;
        }
    }

    public void accumulate(WeightAvgAcc acc, Long iValue, int iWeight) {
        acc.count += iWeight;
        acc.sum += iValue * iWeight;
    }

    public void retract(WeightAvgAcc acc, Long iValue, int iWeight) {
        acc.count -= iWeight;
        acc.sum -= iValue * iWeight;
    }

    public void merge(WeightAvgAcc acc, Iterable<WeightAvgAcc> it) {
        Iterator<WeightAvgAcc> iter = it.iterator();
        while (iter.hasNext()) {
            WeightAvgAcc tmp = iter.next();
            acc.sum += tmp.sum;
            acc.count += tmp.count;
        }
    }

    public void resetAccumulator(WeightAvgAcc acc) {
        acc.count = 0;
        acc.sum = 0L;
    }
}


