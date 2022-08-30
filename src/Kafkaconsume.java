import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.mysql.cj.jdbc.Driver;
import org.json.JSONObject;

import java.util.Scanner;
import java.sql.*;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Kafkaconsume {
    public static void main(String[] args) {
        Connection conn=null;
        Statement stmt =null;

            KafkaConsumer consumer;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put("group.id", "test");
            consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList("TempMonitoring"));
        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records) {
                String Data=record.value();
                System.out.println(record.value());
                JSONObject jsonObject = new JSONObject(record.value());
                System.out.println(String.valueOf(jsonObject));
                String getTemp = String.valueOf(jsonObject.getInt("temp"));
                String getHum = String.valueOf(jsonObject.getInt("humidity"));
                System.out.println(getTemp);

                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn=(Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/iot_db","root","");
                    System.out.println("connection created");
                    stmt=(Statement) conn.createStatement();
//                    String querry ="INSERT INTO `temperature`(`tempertaure`) VALUES ("+getTemp+")";
                    String querry ="INSERT INTO `temperature`(`tempertaure`, `humidity`) VALUES ("+getTemp+","+getHum+")";
                    System.out.println(querry);
                    stmt.executeUpdate(querry);
                    System.out.println("Temperature is: " + Data );


                }
                catch (Exception e){
                    System.out.println("check connection!!!!");
                    System.out.println(e);
                }
            }



        }
    }
}







