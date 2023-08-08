package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class TableTableJoinDemo {
    private static final String bservers = "master:9092";
    private static final String appName = "ttjapp";
    private static final String playerTopic = "player-team";
    private static final String lastScoreTopic = "player-score";
    private static final String outputTopic = "player-team-score";

    public static void main(String[] args) {

        // create streams configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> playerTeamsTable = builder.table(playerTopic);
        KTable<String, Long> playerScoresTable = builder.table(lastScoreTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));
        playerTeamsTable.join(playerScoresTable, (team, score) -> team + "/" + score)
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Topology streamTopology = builder.build();
        KafkaStreams streams = new KafkaStreams(streamTopology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException iex) {
                iex.printStackTrace();
            }
        }
    }
}
