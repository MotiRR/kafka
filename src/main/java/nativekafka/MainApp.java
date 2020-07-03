package nativekafka;

import nativekafka.kafka.MessageWorker;

public class MainApp {

    public static void main(String[] args) {
        MessageWorker messageWorker = new MessageWorker();

        String fileNameProducer = "src\\main\\resources\\config\\producer\\1.properties";
        messageWorker.sendMessage(100, 1, "stu", fileNameProducer);

        String fileNameConsumer = "src\\main\\resources\\config\\consumer\\1.properties";
        messageWorker.readMessage(1, "Offset", fileNameConsumer);
    }
}
