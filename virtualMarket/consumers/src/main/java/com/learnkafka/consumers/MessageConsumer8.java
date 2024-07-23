package com.learnkafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MessageConsumer8 {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer8.class);

    KafkaConsumer<String, String> kafkaConsumer;
    String topicName = "test-topic";
    BufferedWriter writer;
    File file;

    // Variables for summary
    long totalProcessingTime = 0;
    long totalTasks = 0;
    long consumerNumber = 9;
    Set<Integer> partitions = new HashSet<>();
    long sleepBetweenMessages = 133;
    long summaryPosition = -1;

    public MessageConsumer8(Map<String, Object> propsMap, String dirPath) {
        kafkaConsumer = new KafkaConsumer<>(propsMap);
        try {
            String fileName = "Consumer_number_" + consumerNumber + ".txt";
            file = new File(dirPath, fileName);
            writer = new BufferedWriter(new FileWriter(file, true));
        } catch (IOException e) {
            logger.error("Failed to open file for writing: " + e);
        }
    }

    private String getCurrentTimestamp() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
        return LocalDateTime.now().format(formatter);
    }

    public static Map<String, Object> buildConsumerProperties() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageConsumer");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return propsMap;
    }

    public void pollKafka() {
        kafkaConsumer.subscribe(Collections.singleton(topicName));
        Duration timeOutDuration = Duration.of(100, ChronoUnit.MILLIS);
        try {
            boolean isFirstSummaryWrite = true;
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(timeOutDuration);
                consumerRecords.forEach(record -> {
                    String value = record.value();
                    String[] parts = value.split("-", 2);
                    if (parts.length < 2) {
                        logger.error("Invalid message format: {}", value);
                        return;
                    }
                    long sentTimestamp = Long.parseLong(parts[0]);
                    String task = parts[1];

                    LocalDateTime sentTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(sentTimestamp), ZoneId.systemDefault());
                    LocalDateTime receivedTime = LocalDateTime.now();
                    long startTime = System.currentTimeMillis();

                    logger.info("Consumed Record value is {}", value);
                    try {
                        writer.write("Task: " + task + ", Sent at: " + sentTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + ", ");
                        writer.flush();

                        // Extract the sleep duration from the task
                        int sleepDuration = Integer.parseInt(task.split("\\(")[1].replace(")", ""));
                        Thread.sleep(sleepDuration);

                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        totalProcessingTime += totalTime;
                        totalTasks++;
                        partitions.add(record.partition());

                        writer.write("Finished at: " + receivedTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + ", Total time: " + totalTime + " ms\n");
                        writer.flush();
                    } catch (IOException | InterruptedException e) {
                        logger.error("Failed to process message: " + e);
                    }
                });

                // Write or update the summary
                updateSummary(isFirstSummaryWrite);
                isFirstSummaryWrite = false;
            }
        } catch (Exception e) {
            logger.error("Exception in pollKafka: " + e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                logger.error("Failed to close the file writer: " + e);
            }
            kafkaConsumer.close(); // always close the consumer for releasing the connections and sockets
        }
    }

    private void updateSummary(boolean isFirstSummaryWrite) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            if (isFirstSummaryWrite) {
                raf.seek(file.length());
                summaryPosition = raf.getFilePointer();
                raf.writeBytes("\nSummary:\n");
                raf.writeBytes("Consumer number: " + consumerNumber + "\n");
                raf.writeBytes("Partitions read from: " + partitions + "\n");
                raf.writeBytes("Period of time between messages: " + sleepBetweenMessages + " milliseconds\n");
                raf.writeBytes("Average processing time per task: " + (totalTasks > 0 ? totalProcessingTime / totalTasks : 0) + " milliseconds\n");
                raf.writeBytes("Total processing time: " + totalProcessingTime + " milliseconds\n");
                raf.writeBytes("Total tasks processed: " + totalTasks + "\n\n");
            } else {
                raf.seek(summaryPosition);
                raf.writeBytes("\nSummary:\n");
                raf.writeBytes("Consumer number: " + consumerNumber + "\n");
                raf.writeBytes("Partitions read from: " + partitions + "\n");
                raf.writeBytes("Period of time between messages: " + sleepBetweenMessages + " milliseconds\n");
                raf.writeBytes("Average processing time per task: " + (totalTasks > 0 ? totalProcessingTime / totalTasks : 0) + " milliseconds\n");
                raf.writeBytes("Total processing time: " + totalProcessingTime + " milliseconds\n");
                raf.writeBytes("Total tasks processed: " + totalTasks + "\n\n");
            }
        } catch (IOException e) {
            logger.error("Failed to update summary: " + e);
        }
    }

    public static void main(String[] args) {
        String dirPath = "C:\\Users\\tshafran\\OneDrive - Intel Corporation\\Desktop\\consumerLog"; // Specify your directory path here
        Map<String, Object> propsMap = buildConsumerProperties();
        MessageConsumer8 messageConsumer = new MessageConsumer8(propsMap, dirPath);
        messageConsumer.pollKafka();
    }
}
