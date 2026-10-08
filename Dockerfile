FROM maven:3.9.6-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src src
RUN mvn -q package -DskipTests

FROM eclipse-temurin:11-jre AS producer
WORKDIR /app
COPY --from=build /build/target/flink-star-streaming-1.0.0-all.jar /app/app.jar
COPY ["исходные данные", "/data"]
ENTRYPOINT ["java", "-cp", "/app/app.jar", "ru.bigdata.flink.CsvToKafkaProducer"]

FROM flink:1.18.1-java11 AS flink
COPY --from=build /build/target/flink-star-streaming-1.0.0-all.jar /opt/flink/usrlib/flink-star-streaming.jar