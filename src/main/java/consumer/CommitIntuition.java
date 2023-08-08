package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class CommitIntuition {
    private static final String bservers = "master:9092";
    private static final String topic = "commit-test-topic";
    private static final long pollInterval = 10000L;
    private static final String commitInterval = "20000";
    private static final int maxPollRecords = 5;
    private static final String groupId = "commit-grc";

    private static Properties consumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, commitInterval);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps());
        return consumer;
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        int pollNo = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollInterval));
            pollNo++;
            System.out.println("Poll number " + pollNo + " at: " + new Date(System.currentTimeMillis()) + " got " +
                    " records: " +records.count());
            for (ConsumerRecord<String, String> record : records) {

                System.out.println("Offset: " + record.offset() + ", value: " + record.value());
                Thread.sleep(1000);
            }
        }
    }
}
