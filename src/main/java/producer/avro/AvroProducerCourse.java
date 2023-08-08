package producer.avro;

import com.e4rlearning.Course;
import com.e4rlearning.chapters_record;
import com.e4rlearning.topics_record;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvroProducerCourse {
    private static final String bservers = "master:9092";
    private static final String schemaRegistryUrl = "http://master:8081";
    private static final String topic = "course-avro";

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


        Producer<String, Course> producer = new KafkaProducer<String, Course>(properties);
        List<String> chapters = new ArrayList<>();
        chapters.add("Producers");
        chapters.add("Consumers");

        topics_record topicsRecord = topics_record.newBuilder()
                .setTno("t1")
                .setTopic("Consumer Group")
                .setTopicdesc("Kafka consumer groups")
                .build();
        ArrayList<topics_record> topcisArray = new ArrayList<>();
        topcisArray.add(topicsRecord);

        chapters_record chaptersRecord = chapters_record.newBuilder()
                .setCocsw(true)
                .setCocswh(true)
                .setChapter("consumers")
                .setChapterdesc("Kafka consumer")
                .setHno("h1")
                .setTopics(topcisArray)
                .build();
        ArrayList<chapters_record> chaptersArray = new ArrayList<>();
        chaptersArray.add(chaptersRecord);

        com.e4rlearning.Course course = Course.newBuilder()
                .setCourse("Kafka")
                .setCoursedesc("Kafka expllore")
                .setCno("c1")
                .setChapters(chaptersArray)
                .setCosteuro(10)
                .setCostrupee(800)
                .setCostusd(10)
                .setCourselearn("whatever")
                .build();

        ProducerRecord<String, Course> producerRecord = new ProducerRecord<>(
                topic, course
        );

        System.out.println(course);
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
 kafka-avro-console-consumer --bootstrap-server master:9092 --topic course-avro --property print.key=true  --property  print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
         */
    }
}
