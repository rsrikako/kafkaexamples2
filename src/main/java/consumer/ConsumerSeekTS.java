package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerSeekTS {
    static String bservers = "master:9092";
    static String topicName = "ide-topic";
    private static Properties createConsumerConfiguration() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    // create a consumer from configurations
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,
                String>(createConsumerConfiguration());
        return consumer;
    }

    private static Map<TopicPartition, Long> timestamps = new HashMap<>();

    public static void main(String[] args) {
//        Date currentDate = new java.sql.Date();
        long tstToSearchFrom = Instant.now().toEpochMilli() - 1 * 60 * 1000;
        long tsToSearchTo = Instant.now().toEpochMilli();
        KafkaConsumer<String, String> consumer =
                ConsumerSeekTS.createKafkaConsumer();
        boolean offsetFound = false;
        System.out.println("Timestamps from, to: " + tstToSearchFrom + ", " + tsToSearchTo);
        for (long ts = tstToSearchFrom; ts < tsToSearchTo; ts++) {
            timestamps.put(new TopicPartition(topicName, 0), ts);
            System.out.println("searching for ts " + ts);
            Map<TopicPartition, OffsetAndTimestamp> offsets =
                    consumer.offsetsForTimes(timestamps);
            OffsetAndTimestamp oset = offsets.get(new TopicPartition(topicName, 0));

            if (oset != null) {
                offsetFound = true;
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(oset.offset());
                System.out.println("found offset: " + oset.offset());
                break;
            } else {
                if (ts % 20000 == 0) {
                    System.out.println("now at timestamp: " + ts);
                    System.out.println("could not find offset");
                }
            }

//            offsets.forEach((tp, oss) -> {
//                if(oss != null) {
//                    System.out.println(tp + "," + oss.offset());
//                } else
//                    System.out.println(tp);
//            });

        }
    }

}
