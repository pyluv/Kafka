package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Classname MyConsumer
 * @Description TODO
 * @Date 4/22/2020 5:01 PM
 * @Created by Administrator
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        //props.put("group.id", "test");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkatest1");

        //开启自动提交
        props.put("enable.auto.commit", "true");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //自动提交的延迟
        //props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        //key value的反序列化
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //重置消费者的offset
        //只有当更换组名，或者消息过期时，才会重置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅主题 可以多个
        consumer.subscribe(Arrays.asList("test"));

        //测试情况下消费者无需关闭，通过while（true）阻塞即可
        //consumer.close();

        while (true) {
            //获取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            //解析并打印数据
            for (ConsumerRecord<String, String> record : records)

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }
}

