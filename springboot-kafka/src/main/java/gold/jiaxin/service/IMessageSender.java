package gold.jiaxin.service;

/**
 * TODO
 *
 * @author yanxiaodong
 * @version 1.0
 * @date 2020/10/24 10:35
 */
public interface IMessageSender {
    public void sendMessage(String topic, String key, String message);
}
