package datagen;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @programe: cashstore
 * @description: JsonGenerator
 * @author: lpq
 * @create: 2019-07-25 15:45
 **/
public class JsonGenerator {
    public static void main(String[] args){
        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","dn01:9092,dn02:9092,dn03:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer,String> producer=new KafkaProducer<Integer, String>(prop);

        int msgNo=1;
        String topic="CashPlanRecord2";
        String[] machineNos={"001","002","003","004","005"};
        String[] currencies={"CNY","USD","JPY","EUR","HKD"};
        String[] branches={"02604","03208","02587","32541"};
        String[] tellers={"0260401","0320801","0258701","3254101"};
        int[] faceValues={10,50,100,500};
        Random random=new Random();
        while(true){
            try{
                JSONObject ob=new JSONObject();
                ob.put("machineNo",machineNos[random.nextInt(5)]);
                ob.put("termPaymentDay",
                        new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
                ob.put("currency",currencies[random.nextInt(5)]);
                ob.put("branchNo",branches[random.nextInt(4)]);
                ob.put("tellerID",tellers[random.nextInt(4)]);
                ob.put("operTime",
                        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date()));
                ob.put("cashNum",random.nextInt(1000));
                ob.put("faceValue",faceValues[random.nextInt(4)]);

                producer.send(new ProducerRecord<Integer,String>(topic,msgNo,ob.toString()));
                System.out.println(ob.toString());
            }catch(Exception ex){
                ex.printStackTrace();
            }
            msgNo++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
