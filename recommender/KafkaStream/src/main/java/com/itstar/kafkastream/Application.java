package com.itstar.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        String brokers = "bigdata161:9092";
        String zookeepers = "bigdata161:2181";


        String from = "log";
        String to = "recommender";

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "111");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);


        properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTime.class.getName());


        StreamsConfig streamsConfig = new StreamsConfig(properties);

        //source ==> Processor  ==> sink  构建拓扑结构
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSER",() -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSER");


        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);

        streams.start();

        System.out.println("KafkaStreams go =======>");

    }
}
