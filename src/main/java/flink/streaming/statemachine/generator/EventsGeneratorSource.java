package flink.streaming.statemachine.generator;

import flink.streaming.statemachine.event.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;

public class EventsGeneratorSource extends RichParallelSourceFunction<Event> {
    private final double errorProb;
    private final int delayPerRecordMillis;
    private volatile boolean running = true;

    public EventsGeneratorSource(double errorProb, int delayPerRecordMillis) {
        checkArgument(errorProb >= 0.0 && errorProb <= 1.0, "error probability must be in [0.0, 1.0]");
        checkArgument(delayPerRecordMillis >= 0, "deplay must be >= 0");
        this.errorProb = errorProb;
        this.delayPerRecordMillis = delayPerRecordMillis;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        final EventsGenerator generator = new EventsGenerator(errorProb);
        final int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
        final int min = range * getRuntimeContext().getIndexOfThisSubtask();
        final int max = min + range;

        while (running) {
            sourceContext.collect(generator.next(min, max));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
