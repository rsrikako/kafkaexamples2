package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class ProducerDemoKeysCallback {
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
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // create producer using properties
        KafkaProducer<String, String> producer = new KafkaProducer<String,
                String>(props);

        // send 10 records with keys every five seconds to the ide-topic
        try {
            while (true) {
                generateRandomProducerRecord(topicName, 10).forEach(prec ->
                        producer.send(prec, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                System.out.println("callback received at " + getCurrentTime());
                            }
                        }));
                Thread.sleep(5000);
                // check records in kafka-console-consumer
                // kafka-console-consumer --bootstrap-server master:9092 --topic ide-topic  --property print.key=true  --property print.partition=true --property print.offset=true --property print.timestamp=true --from-beginning
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static ArrayList<ProducerRecord> generateRandomProducerRecord(String topic, int noRecs) {
        String[] cities = new String[]{"Mumbai", "Delhi", "Chennai",
                "Kolkatta", "Jaipur", "Bengaluru", "Hyderabad", "Ludhiana"};
        String[] outcomes = new String[]{"W", "L", "D"};
        ArrayList<ProducerRecord> precs = new ArrayList<>();
        for (int i = 0; i < noRecs; i++) {
            ProducerRecord<String, String> prec =
                    new ProducerRecord<>(topic,
                            cities[new Random().nextInt(cities.length)],
                            outcomes[new Random().nextInt(outcomes.length)]);
            precs.add(prec);
        }
        return precs;
    }

    private static String getCurrentTime() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss:SSS z");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }
}
