FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B -q verify

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -g 10001 worker && adduser -D -u 10001 -G worker worker
WORKDIR /app
COPY --from=build --chown=10001:10001 /build/target/vertx-pull-worker-0.1.0-SNAPSHOT.jar /app/worker.jar
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=65 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]
