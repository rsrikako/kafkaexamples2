package producer.avro;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducer {
    private static final String bservers = "master:9092";
    private static  final String schemaRegistryUrl = "http://master:8081";
    private static final String topic = "customer-avro-1";
    public static void main(String[] args) {
        // args - 0 -bootstrap servers, 1 - schema registry, 2 - topic
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", bservers);
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", schemaRegistryUrl);


        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);


        // use the version 1 schema
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("Raman")
                .setLastName("Shastri")
                .setHeight(177f)
                .setWeight(73f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                topic, customer
        );

        System.out.println(customer);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();
// check records in kafka-avro-console-consumer
        /*
 kafka-avro-console-consumer --bootstrap-server master:9092 --topic customer-avro-1 --property print.key=true  --property print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
         */
    }
}
