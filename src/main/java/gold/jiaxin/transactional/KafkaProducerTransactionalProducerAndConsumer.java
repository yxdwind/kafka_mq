package gold.jiaxin.transactional;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/23 21:03
 */
public class KafkaProducerTransactionalProducerAndConsumer {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = buildKafkaProducer();
        KafkaConsumer<String, String> consumer = buildKafkaConsumer();

        //初始化事务
        producer.initTransactions();

        consumer.subscribe(Arrays.asList("topic01"));
        for (; ; ) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                producer.beginTransaction();
                try {
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        //存储元数据
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));

                        ProducerRecord producerRecord = new ProducerRecord("topic02", record.key(), record.value() + "text");
                        producer.send(producerRecord);
                    }

                    producer.sendOffsetsToTransaction(offsets, "g1");
                    producer.commitTransaction();
                } catch (Exception e) {
                    producer.abortTransaction();
                }
            }
        }
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

    public static KafkaConsumer<String, String> buildKafkaConsumer() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092,");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");

        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_commited");

        return new KafkaConsumer<String, String>(prop);
    }
}
