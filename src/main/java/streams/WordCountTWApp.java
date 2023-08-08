package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WordCountTWApp {
    static String bservers = "master:9092";
    static String appName = "wcapptw";
    static String inputTopic = "wcin";
    static String outputTWTopic = "wcout-tw";
    static String outputSWTopic = "wcout-sw";
    public static void main(String[] args) {
//        String bservers = args[0];
//        String appName = args[1];
//        String inputTopic = args[2];
//        String outputTWTopic = args[3];
//        String outputSWTopic = args[4];

        // build a streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(inputTopic);
        // build a topology
        KTable<Windowed<String>, Long> wordCounts = textLines.mapValues(line -> line.toLowerCase())
                .flatMapValues(line -> Arrays.asList(line.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(15)))
                .count();

        wordCounts.toStream().map(
                (key, count) -> new KeyValue<>(key.toString().substring(1, key.toString().indexOf("@")) +
                        ", " +
                        key.window().startTime() + "->" + key.window().endTime(), count.toString())
        ).to(outputTWTopic, Produced.with(Serdes.String(), Serdes.String()));

        KTable<Windowed<String>, Long> wordCountsSW = textLines.mapValues(line -> line.toLowerCase())
                .flatMapValues(line -> Arrays.asList(line.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(15)).advanceBy(Duration.ofSeconds(5)))
                .count();
        // run the stream
        wordCountsSW.toStream().map(
                (key, count) -> new KeyValue<>(key.toString().substring(1, key.toString().indexOf("@")) +
                        ", " +
                        key.window().startTime() + "->" + key.window().endTime(), count.toString())
        ).to(outputSWTopic, Produced.with(Serdes.String(), Serdes.String()));

//        wordCounts.toStream().to("wco-tw", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams :: close));
    }
}
