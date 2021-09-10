FROM adoptopenjdk/openjdk11:jre-11.0.11_9-alpine
ARG JAR_FILE=target/prototype*jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
