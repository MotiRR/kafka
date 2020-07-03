package springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import springkafka.kafka.MessageReceiver;
import springkafka.kafka.MessageSender;

import java.util.concurrent.TimeUnit;


@EnableKafka
@ComponentScan(basePackages = {"./"})
@PropertySource("classpath:config/messenger.properties")
public class MainApp implements CommandLineRunner {

    /*@Autowired
    MessageSender kafkaProducer;

    @Autowired
    MessageReceiver kafkaConsumer;*/

    @Override
    public void run(String[] args) throws Exception {

    }

    public static void main(String[] args) throws InterruptedException {

        /*ApplicationContext context = new AnnotationConfigApplicationContext(MainApp.class);
        MessageSender producer = context.getBean("messageProducer", MessageSender.class);
        MessageReceiver consumer = context.getBean("messageListener", MessageReceiver.class);
        producer.sendMessage("Spring");*/
        //consumer.getLatch().await(10, TimeUnit.SECONDS);

        ConfigurableApplicationContext context = SpringApplication.run(MainApp.class, args);
        MessageSender producer = context.getBean(MessageSender.class);
        MessageReceiver consumer = context.getBean(MessageReceiver.class);
        producer.sendMessage("Spring");
        consumer.getLatch().await(10, TimeUnit.SECONDS);

        for (int i = 1; i < 3; i++) {
            producer.sendMessageToPartition(i, "key", "Spring with partition");
        }
        consumer.getPartitionLatch().await(20, TimeUnit.SECONDS);

        context.close();
    }

    @Bean
    public MessageSender messageProducer() {
        return new MessageSender();
    }

    @Bean
    public MessageReceiver messageListener() {
        return new MessageReceiver();
    }
}
