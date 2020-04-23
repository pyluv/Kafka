package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Classname TimeInterceptor
 * @Description 在消息发送前将时间戳信息加到消息 value 的最前部
 * @Date 4/22/2020 8:58 PM
 * @Created by Administrator
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //取出数据
        String value = producerRecord.value();
        return new ProducerRecord<String, String>(
                producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.key(),
                System.currentTimeMillis() + "," + producerRecord.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }


}
