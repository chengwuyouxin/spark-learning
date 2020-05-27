package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//自动提交偏移量
//消费者轮询时会先检查是否该提交偏移量了，如果是，则提交上一次轮询返回的偏移量
//若发生再均衡，在上一次提交的偏移量到在均衡时间点之间的消息将被重复处理

public class JavaConsumerDemo {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        properties.put("group.id", "javaconsumerdemo");
        properties.put("auto.offset.reset", "earliest"); //默认是latest
        properties.put("session.timeout.ms", "30000");  //默认是3s
        properties.put("heartbeat.interval.ms", "1000"); //一般是session.timeout.ms的三分之一
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //自动提交偏移量
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");//默认是5s

        KafkaConsumer<String,String> consumer=
                new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));
        try{
            while(true){
                ConsumerRecords<String,String> records=consumer.poll(100);
                for(ConsumerRecord<String,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            consumer.close();
        }

    }
}
