package datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * @programe: SparkExample
 * @description: 将风险相关数据导入Kafka
 * @author: lpq
 * @create: 2019-07-29 08:07
 **/
public class FaceRecgLogGen {
    public static void main(String[] args){
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","dn01:9092,dn02:9092,dn03:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(prop);

        String topic="facerecglog";
        try{
            File file=new File("E://exercise//data//facerecglog");
            FileReader fileReader=new FileReader(file);
            BufferedReader reader=new BufferedReader(fileReader);

            int lineNum=1;
            String lineContent=null;
            while((lineContent=reader.readLine())!=null){
                producer.send(new ProducerRecord<String, String>(
                        topic,"facerecglog"+lineNum,lineContent));
                System.out.println(lineContent);
                Thread.sleep(1);
            }
            lineNum++;
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
