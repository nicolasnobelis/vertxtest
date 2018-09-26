package nino;

import java.io.IOException;

public class KafkaTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaServer serverKafka = new KafkaServer();
        serverKafka.start();

        // TODO logging
    }
}
