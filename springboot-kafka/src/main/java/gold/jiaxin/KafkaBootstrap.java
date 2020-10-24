package gold.jiaxin;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/24 10:14
 */
@SpringBootApplication
public class KafkaBootstrap {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(KafkaBootstrap.class, args);
        System.in.read();
    }

    @KafkaListeners(value = {
            @KafkaListener(topics = {"topic01"})
    })
    public void receive01(ConsumerRecord<String, String> record) {
        System.out.println("record:" + record);
    }

    @KafkaListeners(value = {
            @KafkaListener(topics = {"topic02"})
    })
    @SendTo("topic03")
    public String receive02(ConsumerRecord<String, String> record) {
        return record.value()+"\t --- add";
    }
}
