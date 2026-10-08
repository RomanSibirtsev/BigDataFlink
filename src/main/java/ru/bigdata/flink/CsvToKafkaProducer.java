package ru.bigdata.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class CsvToKafkaProducer {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "sales");
        Path inputDirectory = Path.of(env("INPUT_DIR", "исходные данные"));
        long delayMillis = Long.parseLong(env("MESSAGE_DELAY_MS", "0"));

        List<Path> csvFiles;
        try (var paths = Files.list(inputDirectory)) {
            csvFiles = paths
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
        }
        if (csvFiles.isEmpty()) {
            throw new IllegalArgumentException("No CSV files found in " + inputDirectory);
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        ObjectMapper mapper = new ObjectMapper();
        long sentMessages = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (Path csvFile : csvFiles) {
                try (Reader reader = Files.newBufferedReader(csvFile);
                     CSVParser parser = CSVFormat.DEFAULT.builder()
                             .setHeader()
                             .setSkipHeaderRecord(true)
                             .build()
                             .parse(reader)) {
                    long sourceRow = 0;
                    for (CSVRecord csvRecord : parser) {
                        sourceRow++;
                        SaleEvent event = SaleEvent.from(
                                csvRecord.toMap(), csvFile.getFileName().toString(), sourceRow);
                        String messageKey = event.source_file + ":" + event.source_row;
                        String json = mapper.writeValueAsString(event);
                        producer.send(new ProducerRecord<>(topic, messageKey, json)).get();
                        sentMessages++;
                        if (delayMillis > 0) {
                            Thread.sleep(delayMillis);
                        }
                    }
                }
            }
            producer.flush();
        }
        System.out.printf("Sent %d messages from %d CSV files to topic %s%n",
                sentMessages, csvFiles.size(), topic);
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}