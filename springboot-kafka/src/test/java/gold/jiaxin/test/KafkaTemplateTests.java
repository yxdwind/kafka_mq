package gold.jiaxin.test;

import gold.jiaxin.KafkaBootstrap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/24 10:24
 */
@SpringBootTest(classes = KafkaBootstrap.class)
@RunWith(SpringRunner.class)
public class KafkaTemplateTests {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    //非事务执行
    public void testSendMessage1() {
        kafkaTemplate.send(new ProducerRecord<String, String>("topic02", "002", "this is kafka template"));
    }

    @Test
    //事务下执行
    public void testSendMessage2() {
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<String, String, Object>() {
            @Override
            public Object doInOperations(KafkaOperations<String, String> kafkaOperations) {
                kafkaOperations.send(new ProducerRecord<String, String>("topic02", "topic01", "this is kafka template"));
                return null;
            }
        });
    }
}
