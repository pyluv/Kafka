package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Classname CounterInterceptor
 * @Description TODO
 * @Date 4/22/2020 9:04 PM
 * @Created by Administrator
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int successCounter = 0;
    int errorCounter = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null) {
            successCounter++;
        } else errorCounter++;
    }

    public void close() {
        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }

    public void configure(Map<String, ?> map) {

    }
}
