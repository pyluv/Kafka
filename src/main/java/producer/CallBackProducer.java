package producer;

import org.apache.kafka.clients.producer.*;
import sun.plugin2.jvm.RemoteJVMLauncher;

import java.util.Properties;

/**
 * @Classname CallBackProducer
 * @Description TODO
 * @Date 4/22/2020 4:29 PM
 * @Created by Administrator
 */
public class CallBackProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //指定连接kafka集群
        //props.put("bootstrap.servers", "hadoop102:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        //ACK应答级别
        //props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //key value的序列化类
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //添加分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"producer.MyPartitioner");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //发送数据
        for (int i =0; i<10; i++) {
            producer.send(new ProducerRecord<String, String>("test", "massage" + i), new Callback() {
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(recordMetadata.partition());
                }
            });
        }

        //关闭资源
        producer.close();
    }
}
