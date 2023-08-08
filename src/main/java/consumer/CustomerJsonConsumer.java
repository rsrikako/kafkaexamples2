package consumer;

import json.Customer;
import json.CustomerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomerJsonConsumer {
    private static String bootStrapServers = "master:9092";
    private static String topicName = "customer-topic";
    private static String customerGroup = "customer-group1";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(CustomerJsonConsumer.class.getName());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, customerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singleton(topicName));
        DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
        while(true){
            ConsumerRecords<String, Customer> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Customer> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("birthdate " + df.format(record.value().getBirthDate()));
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
}