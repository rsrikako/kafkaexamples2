package mtkco;

public class MTKCOMain {
    static String bservers = "master:9092";
    static String topicName = "ide-topic";
    public static void main(String[] args) {
        MTKCO mtkco = new MTKCO(bservers, topicName);
        mtkco.run();
    }
}
