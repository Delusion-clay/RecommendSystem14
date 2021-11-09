package com.itstar.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    /**
     * @param key   文件名
     * @param value 实际数据
     */
    @Override
    public void process(byte[] key, byte[] value) {
        //MOVIE_RATING_PREFIX:370|2770|4.0|835355511
        String input = new String(value);
        if (input.contains("MOVIE_RATING_PREFIX:")) {
            //370|2770|4.0|835355511  ==> to ==> recommender [Topic]
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();

            context.forward("logProcessor".getBytes(),input.getBytes());
        }

    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
