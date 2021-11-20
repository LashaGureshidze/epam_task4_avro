package com.epam.task4;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class Producer {

    private static Properties props;
    private static Schema schema;
    private static KafkaProducer<Object, Object> producer;

    public static void main(String[] args) {

        initKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("shutting down");
                producer.close();
            }
        });

        //send messages every 5 seconds
        while (true) {
            sendRandomMessage();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendRandomMessage() {

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("message", "hey " + new Random().nextInt(1000));
        avroRecord.put("timestamp", System.currentTimeMillis());

        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", avroRecord);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("metadata (" + recordMetadata.toString() + ")");
                } else {
                    e.printStackTrace();
                }
            }
        });

        producer.flush();
    }

    private static void initKafkaProducer() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "5");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        props.put("request.timeout.ms", 190000);

        String userSchema = "{\n" +
                "  \"namespace\": \"com.epam.task4.stubs\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Ping\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"message\", \"type\": \"string\"},\n" +
                "    {\"name\": \"timestamp\",  \"type\": \"long\"}\n" +
                "  ]\n" +
                "}";
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(userSchema);

        producer = new KafkaProducer<>(props);
    }
}
