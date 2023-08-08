package streams;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Properties;

public class StreamStreamJoinDemo {
static String bservers = "master:9092";
static String appName = "ssjapp";
static String streamt1 = "impressions";
static String streamt2 = "clicks";
static String ssjtopic = "impressions-and-clicks";
    public static void main(String[] args) {
//        String bservers = args[0];
//        String appName = args[1];

        // create streams configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"1000");
//        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/vagrant/kstapp");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> alerts = builder.stream(streamt1);
        KStream<String, String> incidents = builder.stream(streamt2);

        final KStream<String, String> impressionsAndClicks = alerts.outerJoin(
                incidents, (impv, clickv) ->
                        (clickv == null) ? impv + "/not-clicked-yet": impv + "/" + clickv,
                JoinWindows.of(Duration.ofSeconds(60)),
                StreamJoined.with(
                        Serdes.String(), Serdes.String(), Serdes.String()
                )
        );

        impressionsAndClicks.to(ssjtopic);
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
