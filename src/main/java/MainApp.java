import kafka.MessageWorker;

public class MainApp {

    public static void main(String[] args) throws InterruptedException {
        MessageWorker messageWorker = new MessageWorker();
        //messageWorker.sendMessage(10000, 4);
        messageWorker.readMessage(2, true);
    }
}
