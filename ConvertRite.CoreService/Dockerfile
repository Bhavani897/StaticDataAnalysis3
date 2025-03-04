FROM maven:3.8.3-openjdk-17-slim AS MAVEN_BUILD
MAINTAINER Phaneendra Satyavolu
COPY pom.xml /app/
COPY src /app/src
RUN mvn -f /app/pom.xml clean package -e -DskipTests
FROM openjdk:17.0.1-jdk-slim
COPY --from=MAVEN_BUILD /app/target/convert-rite-api-0.0.1-SNAPSHOT.jar /app/
RUN apt-get update
RUN apt-get install net-tools
# this volume is mounted so that external configuration can be loaded
VOLUME /app/config

ARG SPRING_PROFILES_ACTIVE=gold
ENV SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE}
#adjusting the entrypoint accordingly to take in application props from external
ENTRYPOINT ["java", "-jar", "-Dspring.config.additional-location=/app/config/", "app/convert-rite-api-0.0.1-SNAPSHOT.jar"]
EXPOSE 9000
 
