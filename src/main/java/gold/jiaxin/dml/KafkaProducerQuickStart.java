package gold.jiaxin.dml;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 21:03
 */
public class KafkaProducerQuickStart {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic01", "key" + i, "value" + i);
            //发送消息给服务器
            producer.send(record);
        }

        producer.close();
    }
}
