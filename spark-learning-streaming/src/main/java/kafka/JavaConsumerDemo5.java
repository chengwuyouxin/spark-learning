package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//同步提交和异步提交组合使用
//在正常消费过程中使用异步提交，如果提交失败可以不断重试
//当发生异常或者消费完毕时，在关闭consumer前使用同步提交，确保提交最新的偏移量，若此时提交错误，可抛出或者记录异常

public class JavaConsumerDemo5 {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo5");

        props.put("auto.commit.offset","false");

        KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer,String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));

        try{
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                }
                consumer.commitAsync();
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally {
            try{
                consumer.commitSync();
            }catch(Exception ex){
                ex.printStackTrace();
            }finally{
                consumer.close();
            }
        }
    }
}
