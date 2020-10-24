package gold.jiaxin.test;

import gold.jiaxin.KafkaBootstrap;
import gold.jiaxin.service.IMessageSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
public class MessageSenderTests {
    @Autowired
    private IMessageSender iMessageSender;

    @Test
    //非事务执行
    public void testSendMessage1() {
        iMessageSender.sendMessage("topic02", "002", "this is iMessageSender");
    }

}
