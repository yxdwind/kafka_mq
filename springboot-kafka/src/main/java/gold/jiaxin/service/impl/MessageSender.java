package gold.jiaxin.service.impl;

import gold.jiaxin.service.IMessageSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/24 10:36
 */
@Service
@Transactional
public class MessageSender implements IMessageSender {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendMessage(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
    }
}
