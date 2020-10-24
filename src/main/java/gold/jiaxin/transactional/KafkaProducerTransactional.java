package gold.jiaxin.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 21:03
 */
public class KafkaProducerTransactional {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = buildKafkaProducer();

        //初始化事务
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                if (i == 8) {
                    int j = 1 / 0;
                }
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic01", "key" + i, "value" + i);
                //发送消息给服务器
                producer.send(record);
                producer.flush();
            }
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        }

        producer.close();
    }

    public static KafkaProducer<String, String> buildKafkaProducer() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id" + UUID.randomUUID().toString());
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        //等待5ms 如果batch中数据不足1024 大小 等待5ms就发送出去
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        return new KafkaProducer<String, String>(prop);
    }
}
