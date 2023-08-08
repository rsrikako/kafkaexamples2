package streams;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class TableTableJoinDriver {
    private static final String bservers = "master:9092";
    private static final String teamsTopic = "player-team";
    private static final String scoreTopic = "player-score";

    public static void main(String[] args) {
       
        Properties pTProps = new Properties();
        pTProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        pTProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        pTProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // create producer using properties
        KafkaProducer<String, String> producerPT = new KafkaProducer<String,
                String>(pTProps);

        Properties pSProps = new Properties();
        pSProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        pSProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        pSProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());

        // create producer using properties
        KafkaProducer<String, Long> producerPS = new KafkaProducer<String,
                Long>(pSProps);

        for(ProducerRecord<String, String> ptrec: createPTRecords()){
            producerPT.send(ptrec, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("player team record sent successfully");
                }
            });
        }
        producerPT.close();

        for(ProducerRecord<String, Long> psrec: createPSRecords()){
            producerPS.send(psrec, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("player score record sent successfully");
                }
            });
        }
        producerPS.close();
    }
    static ArrayList<ProducerRecord<String, String>> createPTRecords(){
        ArrayList<ProducerRecord<String, String>> pTRecs = new ArrayList<>();
        pTRecs.add(new ProducerRecord<>(teamsTopic,"Kevin Pollard","mumbai"));
        pTRecs.add(new ProducerRecord<>(teamsTopic,"Kevin Pollard","chennai"));
        pTRecs.add(new ProducerRecord<>(teamsTopic,"Shikhar Dhawan","delhi"));
        pTRecs.add(new ProducerRecord<>(teamsTopic,"Shikhar Dhawan","punjab"));
        pTRecs.add(new ProducerRecord<>(teamsTopic,"David Warner","hyderabad"));
        return pTRecs;
    }
    static ArrayList<ProducerRecord<String, Long>> createPSRecords(){
        ArrayList<ProducerRecord<String, Long>> pSRecs = new ArrayList<>();
        pSRecs.add(new ProducerRecord<>(scoreTopic,"Kevin Pollard",87L));
        pSRecs.add(new ProducerRecord<>(scoreTopic,"Kevin Pollard", 43L));
        pSRecs.add(new ProducerRecord<>(scoreTopic,"Shikhar Dhawan",29L));
        pSRecs.add(new ProducerRecord<>(scoreTopic,"Shikhar Dhawan", 47L));
        pSRecs.add(new ProducerRecord<>(scoreTopic,"David Warner", 123L));
        return pSRecs;
    }
}
