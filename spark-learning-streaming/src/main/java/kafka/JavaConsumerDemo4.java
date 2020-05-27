package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

//异步提交
//在成功提交之前，commitAsync会一直重试，如果第一次提交时失败，
//之后更大的偏移量提交成功，当第二次重试提交成功，若此时发生再均衡，也会发生数据重复

public class JavaConsumerDemo4 {
    public static void main(String[] args){
        Properties props=new Properties();
        props.put("bootstrap.servers", "dn01:9092,dn02:9092,dn03:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");
        props.put("group.id","javaconsumerdemo4");

        //手工提交偏移量需要设置为false
        props.put("auto.commit.offset","false");

        KafkaConsumer<Integer,String> consumer=new KafkaConsumer<Integer, String>(props);
        consumer.subscribe(Arrays.asList("JavaProducerDemo2"));

        try{
            while(true){
                ConsumerRecords<Integer,String> records=consumer.poll(100);
                for(ConsumerRecord<Integer,String> record:records){
                    System.out.println(String.format("partition: %d,offerset: %d,key:%s,value:%s",
                            record.partition(),record.offset(),record.key(),record.value()));
                }
                //consumer.commitAsync();
                //异步提交增加回调，失败时记录相关信息
                consumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if(e!=null){
                            System.out.println("Commit failed for offsets:"+map.toString());
                        }
                    }
                });
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
