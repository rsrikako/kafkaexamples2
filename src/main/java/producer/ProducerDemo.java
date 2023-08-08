package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class ProducerDemo {
    private static String bootStrapServers = "master:9092";
    private static String topicName = "ide-topic";

    public static void main(String[] args) {
        // create properties for producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        // create producer using properties
        KafkaProducer<String, String> producer = new KafkaProducer<String,
                String>(props);

        // create a producer records
        ProducerRecord<String, String> record = new ProducerRecord<String,
                String>(topicName, "hello from the ide");

        // send the records to the broker
        producer.send(record);
        producer.flush();

        // check records in kafka-console-consumer
        // kafka-console-consumer --bootstrap-server master:9092 --topic ide-topic  --property print.key=true  --property print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
    }
}
