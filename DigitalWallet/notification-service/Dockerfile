FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -pl notification-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/notification-service/target/notification-service-0.0.1-SNAPSHOT.jar app.jar
USER 10001
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
