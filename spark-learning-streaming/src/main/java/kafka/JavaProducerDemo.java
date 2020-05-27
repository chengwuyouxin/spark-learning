package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class JavaProducerDemo {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");//xxx服务器ip
//        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
//        props.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
//        props.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.
//                                          默认的批量处理消息字节数
//        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
//        props.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，
// 不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
//        props.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props);

        int messageNo=1;
        String topic="JavaProducerDemo";
        while(messageNo<=5){
            String message=String.format("Kafka Producer produce %s message",
                    String.valueOf(messageNo));

            //只是发送，不关心是否正常到达
            try{
                producer.send(new ProducerRecord<Integer, String>(topic,
                        messageNo,message));
            }catch(Exception e){
                e.printStackTrace();
            }
            messageNo++;
        }
    }
}
