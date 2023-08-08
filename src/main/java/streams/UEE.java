package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class UEE {
    private  static final String bservers = "master:9092";
    private  static final String appName = "ueapp";
    private static final String userPurchasesTopic = "userp";
    private  static final String userTableTopic = "usert";
    private static final String userEETopic = "upej";
    private static final String userEELJTopic = "upelj";

    public static void main(String[] args) {
        // configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Streams builder
        StreamsBuilder builder = new StreamsBuilder();
        // create stream
        KStream<String, String> userPurchases = builder.stream(userPurchasesTopic);
        // create global ktable
        // This table will be replicated on each kafka streams application
        GlobalKTable<String, String> userInfo = builder.globalTable(userTableTopic);

        // stream join globaltable
        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(userInfo,
                (key, value) -> key,
                (purchase, user) -> purchase + ", user  info: " + user);
        userPurchasesEnrichedJoin.to(userEETopic);

        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(userInfo,
                (key, value) -> key,
                (userPurchase, userinfo) -> {
                    // as this is a left join, userInfo can be null
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ",UserInfo=[" + userinfo + "]";
                    } else {
                        return "Purchase=" + userPurchase + ", UserInfo=null";
                    }
                });

        userPurchasesEnrichedLeftJoin.to(userEELJTopic);

        // streams start
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
