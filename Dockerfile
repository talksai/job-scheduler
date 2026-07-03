FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY scheduler-store/pom.xml scheduler-store/
COPY scheduler-coordinator/pom.xml scheduler-coordinator/
COPY scheduler-observability/pom.xml scheduler-observability/
COPY scheduler-core/pom.xml scheduler-core/
COPY scheduler-execution/pom.xml scheduler-execution/
COPY scheduler-api/pom.xml scheduler-api/
RUN mvn -B -q dependency:go-offline || true
COPY scheduler-store scheduler-store
COPY scheduler-coordinator scheduler-coordinator
COPY scheduler-observability scheduler-observability
COPY scheduler-core scheduler-core
COPY scheduler-execution scheduler-execution
COPY scheduler-api scheduler-api
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/scheduler-api/target/scheduler-api-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
