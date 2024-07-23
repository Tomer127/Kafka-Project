//package com.learnkafka.producer;
//
//import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeoutException;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//
//public class MessageProducer {
//
//    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
//
//    String topicName = "test-topic";
//    KafkaProducer<String, String> kafkaProducer;
//
//    public MessageProducer(Map<String, Object> producerProps) {
//        kafkaProducer = new KafkaProducer<>(producerProps);
//    }
//
//    Callback callback = (recordMetadata, exception) -> {
//        if (exception != null) {
//            logger.error("Exception is {} ", exception.getMessage());
//        } else {
//            logger.info("Record MetaData Async in CallBack Offset : {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
//        }
//    };
//
//    public void close() {
//        kafkaProducer.close();
//    }
//
//    public void publishMessageSync(String key, String message) {
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
//        RecordMetadata recordMetadata = null;
//        try {
//            recordMetadata = kafkaProducer.send(producerRecord).get();
//            logSuccessResponse(message, key, recordMetadata);
//        } catch (InterruptedException | ExecutionException e) {
//            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
//        }
//    }
//
//    public void logSuccessResponse(String message, String key, RecordMetadata recordMetadata) {
//        logger.info("Message ** {} ** sent successfully with the key  **{}** . ", message, key);
//        logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
//    }
//
//    public static Map<String, Object> propsMap() {
//        Map<String, Object> propsMap = new HashMap<>();
//        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
//        propsMap.put(ProducerConfig.RETRIES_CONFIG, "10");
//        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");
//
//        return propsMap;
//    }
//
//    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
//        // Initialize Kafka producer properties
//        Map<String, Object> producerProps = propsMap();
//        MessageProducer messageProducer = new MessageProducer(producerProps);
//
//        // Path to the text file
//        String filePath = "C:\\Users\\tshafran\\OneDrive - Intel Corporation\\Desktop\\producerDataBase\\math.txt";
//
//        // Read from the text file
//        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                // Send each line as a separate message
//                messageProducer.publishMessageSync(null, line);
//                // Optional: Sleep to ensure messages are sent before exiting
//                Thread.sleep(300);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            messageProducer.close();
//        }
//    }
//}









//package com.learnkafka.producer;
//
//import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeoutException;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//
////import static com.learnkafka.producer.ItemProducer.logger;
//
//public class MessageProducer {
//
//    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
//
//    String topicName = "test-topic";
////    String topicName = "test-topic-replicated";
//    KafkaProducer kafkaProducer;
//
//    public MessageProducer(Map<String, Object> producerProps) {
//        kafkaProducer = new KafkaProducer(producerProps);
//    }
//
//    Callback callback = (recordMetadata, exception) -> {
//        if (exception != null) {
//            logger.error("Exception is {} ", exception.getMessage());
//        } else {
//            logger.info("Record MetaData Async in CallBack Offset : {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
//        }
//    };
//
//    public void close(){
//        kafkaProducer.close();
//    }
//
//
//    public void publishMessageSync(String key, String message) {
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
//        RecordMetadata recordMetadata = null;
//        try {
//            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
//            logSuccessResponse(message, key,  recordMetadata);
//        } catch (InterruptedException | ExecutionException e) {
//            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
//        }
//
//    }
//
//    public void logSuccessResponse(String message, String key, RecordMetadata recordMetadata) {
//        logger.info("Message ** {} ** sent successfully with the key  **{}** . ", message, key);
//        logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
//    }
//
////    public void publishMessageAsync(String key, String message) throws InterruptedException, ExecutionException, TimeoutException {
////        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
////        kafkaProducer.send(producerRecord, callback);
////        // kafkaProducer.send(producerRecord).get();
////    }
//
//    public static Map<String, Object> propsMap() {
//
//        Map<String, Object> propsMap = new HashMap<>();
//        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        //msesader hodaot lefi bitim.
//        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        //
//        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
//        propsMap.put(ProducerConfig.RETRIES_CONFIG, "10");
//        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");
//
//        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
//        return propsMap;
//    }
//
//    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
//        // Initialize Kafka producer properties
//        Map<String, Object> producerProps = propsMap();
//        MessageProducer messageProducer = new MessageProducer(producerProps);
//
//        // Path to the text file
//        String filePath = "C:\\Users\\tshafran\\OneDrive - Intel Corporation\\Desktop\\producerDataBase\\tasks.txt";
//
//        // Read from the text file
//        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                // Split the line into words
//                String[] words = line.split("\\s+");
//                for (String word : words) {
//                    // Send each word as a separate message
//                    messageProducer.publishMessageSync("A", word);
//                    // Optional: Sleep to ensure messages are sent before exiting
//                    Thread.sleep(300);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//}



package com.learnkafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    String topicName = "test-topic";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> producerProps) {
        kafkaProducer = new KafkaProducer<>(producerProps);
    }

    Callback callback = (recordMetadata, exception) -> {
        if (exception != null) {
            logger.error("Exception is {} ", exception.getMessage());
        } else {
            logger.info("Record MetaData Async in CallBack Offset : {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
        }
    };

    public void close() {
        kafkaProducer.close();
    }

    public void publishMessageSync(String key, String message) {
        String messageWithTimestamp = System.currentTimeMillis() + "-" + message;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, messageWithTimestamp);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
            logSuccessResponse(messageWithTimestamp, key, recordMetadata);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception in publishMessageSync : {} ", e.getMessage());
        }
    }

    public void logSuccessResponse(String message, String key, RecordMetadata recordMetadata) {
        logger.info("Message ** {} ** sent successfully with the key  **{}** . ", message, key);
        logger.info(" Published Record Offset is {} and the partition is {}", recordMetadata.offset(), recordMetadata.partition());
    }

    public static Map<String, Object> propsMap() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
        propsMap.put(ProducerConfig.RETRIES_CONFIG, "10");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");
        return propsMap;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        // Initialize Kafka producer properties
        Map<String, Object> producerProps = propsMap();
        MessageProducer messageProducer = new MessageProducer(producerProps);

        // Path to the text file
        String filePath = "C:\\Users\\tshafran\\OneDrive - Intel Corporation\\Desktop\\producerDataBase\\tasks.txt";

        // Read from the text file
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Send the line as a separate message
                messageProducer.publishMessageSync(null, line);
                // Optional: Sleep to ensure messages are sent before exiting
                Thread.sleep(133);            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        messageProducer.close();
    }
}

