package flink.streaming.statemachine.generator;

import flink.streaming.statemachine.event.Event;
import org.apache.flink.util.Collector;

import java.util.Collection;


public class StandaloneThreadedGenerator {

    public static void runGenerator(Collector<Event>[] collectors) {
        final GeneratorThread [] threads = new GeneratorThread[collectors.length];
        final int range = Integer.MAX_VALUE / collectors.length;

        for (int i = 0; i<threads.length; i++) {
            int min = range * i;
            int max = min + range;
            GeneratorThread thread = new GeneratorThread(collectors[i], min, max);
            threads[i] = thread;
            thread.setName("Generator " + i);
        }

        long delay = 2L;

    }

    private static class GeneratorThread extends Thread {
        private final Collector<Event> out;
        private final int minAddress;
        private final int maxAddress;
        private long delay;
        private long count;
        private volatile boolean running;
        private volatile boolean injectInvalidNext;

        GeneratorThread(Collector<Event> out, int minAddress, int maxAddress) {
            this.out = out;
            this.minAddress = minAddress;
            this.maxAddress = maxAddress;
            this.running = true;
        }

        @Override
        public void run() {
            final EventsGenerator generator = new EventsGenerator();
            while (running) {
                if (injectInvalidNext) {
                    injectInvalidNext = false;
                    Event next = generator.nextInvalid();
                    if (next != null)
                        out.collect(next);
                } else {
                    out.collect(generator.next(minAddress, maxAddress));
                }
                count += 1;
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        public long currentCount() {
            return count;
        }

        public void shutdown() {
            running = false;
            interrupt();
        }

        public void setDelay(long delay) {
            this.delay = delay;
        }

        public void sendInvalidStateTransition() {
            injectInvalidNext = true;
        }
    }

    private static class ThroughputLogger extends Thread {
        private final GeneratorThread[] generators;
        private volatile boolean running;

        public ThroughputLogger(GeneratorThread[] generators, boolean running) {
            this.generators = generators;
            this.running = running;
        }

        @Override
        public void run() {
            long lastCount = 0L;
            long lastTimeStamp = System.currentTimeMillis();

            while (running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }

                long ts = System.currentTimeMillis();
                long curCount = 0L;
                for (GeneratorThread generator : generators)
                    curCount += generator.currentCount();

                double factor = (ts - lastTimeStamp)/1000;
                double perSec = (curCount - lastCount)/factor;

                lastTimeStamp = ts;
                lastCount = curCount;

                System.out.println(perSec + " /Sec");
            }
        }

        public void shutDown() {
            running = false;
            interrupt();
        }
    }
}
