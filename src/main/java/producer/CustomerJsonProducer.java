package producer;

import json.Customer;
import json.CustomerSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class CustomerJsonProducer {
    private static String bootStrapServers = "master:9092";
    private static String topicName = "customer-topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class.getName());

        KafkaProducer<String, Customer> producer = new KafkaProducer<String, Customer>(props);

        for(int n = 1; n <= 10; n++){
            Customer customer = new Customer(n, "Customer " + n, new Date());
            ProducerRecord record = new ProducerRecord(topicName, "id " + n, customer);
            producer.send(record);
        }

        producer.flush();
        producer.close();
        // check records in kafka-console-consumer
        // kafka-console-consumer --bootstrap-server master:9092 --topic customer-topic  --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
    }
}
