package producer.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class GenericRecordProducer {
    private static final String bservers = "master:9092";
    private static  final String schemaRegistryUrl = "http://master:8081";
    private static final String topic = "generic-consumer";
    public static void main(String[] args) {
        Schema.Parser parser = new Schema.Parser();
        String genericSchemaString = "{\n" +
                "    \"type\": \"record\",\n" +
                "    \"namespace\": \"com.example\",\n" +
                "    \"name\": \"GCustomer\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"first_name\", \"type\": \"string\",\"doc\": \"Customer first name\" },\n" +
                "        {\"name\": \"last_name\", \"type\": \"string\",\"doc\": \"Customer last name\"},\n" +
                "        {\"name\":\"age\", \"type\": \"int\",\"doc\":\"Age\"},\n" +
                "        {\"name\":\"height\", \"type\": \"float\"},\n" +
                "        {\"name\":\"likes\", \"type\": {\"type\": \"array\", \"items\": \"string\", \"default\": []}}\n" +
                "    ]\n" +
                "}";
        Schema schema = parser.parse(genericSchemaString);
        GenericRecord genericCustomerRecord = new GenericData.Record(schema);
        genericCustomerRecord.put("first_name", "john");
        genericCustomerRecord.put("last_name", "gulliver");
        genericCustomerRecord.put("age", 19);
        genericCustomerRecord.put("height", 183.4f);
        List<String> likes = new ArrayList<>();
        likes.add("travelling");
        likes.add("preaching");
        genericCustomerRecord.put("likes", likes);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, genericCustomerRecord);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("Sent record to topic: " + metadata.topic() + ", partition: " + metadata.partition() + ", offset: " + metadata.offset());
            }
        });
        producer.close();
        // check records in kafka-avro-console-consumer
        /*
        kafka-avro-console-consumer --bootstrap-server master:9092 --topic generic-consumer  --property print.key=true --property print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
         */
    }
}
