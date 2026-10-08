package ru.bigdata.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class FlinkStarJob {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "sales");
        String jdbcUrl = env("POSTGRES_URL", "jdbc:postgresql://localhost:5432/sales");
        String jdbcUser = env("POSTGRES_USER", "sales");
        String jdbcPassword = env("POSTGRES_PASSWORD", "sales");

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.enableCheckpointing(10_000);

        KafkaSource<SaleEvent> source = KafkaSource.<SaleEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-star-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonSaleDeserializer())
                .build();

        DataStream<StarRecord> records = execution
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka sales source")
                .map(StarRecord::from)
                .name("Transform events to star model");

        records.addSink(new PostgresStarSink(jdbcUrl, jdbcUser, jdbcPassword))
                .name("PostgreSQL star schema sink")
                .setParallelism(1);
        execution.execute("Kafka sales to PostgreSQL star schema");
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    public static class JsonSaleDeserializer extends AbstractDeserializationSchema<SaleEvent> {
        private transient ObjectMapper mapper;

        @Override
        public SaleEvent deserialize(byte[] message) throws IOException {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(message, SaleEvent.class);
        }
    }
}