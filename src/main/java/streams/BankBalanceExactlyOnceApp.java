package streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.codehaus.jackson.JsonFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class BankBalanceExactlyOnceApp {
    private static final String bservers = "master:9092";
    private static final String appName = "bbeo-app";
    private static final String inputTopic = "bank-transactions";
    private  static final String outputTopic = "bank-balance-exactly-once";
    public static void main(String[] args) {
        // create streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bservers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.POLL_MS_CONFIG, "10");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10");
        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), jsonSerde));
//                builder.stream(Serdes.String(), jsonSerde, "bank-transactions");


        // create the initial json object for balances
        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

//        Previous working bankBalance
//        KTable<String, JsonNode> bankBalance = bankTransactions
//                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
//                .aggregate(
//                        () -> initialBalance,
//                        (key, transaction, balance) -> newBalance(transaction, balance),
//                        Materialized.as("bank-balance-agg").with(Serdes.String(), jsonSerde)
//                );

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg").with(Serdes.String(), jsonSerde)
                );

        bankBalance.toStream().to(outputTopic, Produced.with(Serdes.String(), jsonSerde));

//        bankTransactions
//                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
//                .windowedBy(SessionWindows.with(Duration.ofMinutes(2)))
//                .reduce((jn1, jn2) -> {
//                    ObjectNode onode = JsonNodeFactory.instance.objectNode();
//                    onode.put("balacne", jn1.get("balance").asInt() + jn2.get("balance").asInt());
//                    onode.put("count", jn1.get("count").asInt() + 2);
//                    return onode;
//                })
//                .toStream();

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//        streams.cleanUp();
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
