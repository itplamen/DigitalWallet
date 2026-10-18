FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -pl query-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/query-service/target/query-service-0.0.1-SNAPSHOT.jar app.jar
USER 10001
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
