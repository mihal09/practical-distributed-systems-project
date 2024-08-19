FROM debian:bullseye-slim

RUN apt-get update && \
	apt-get install -y --no-install-recommends openjdk-11-jre-headless maven

COPY ["processor/", "/opt/processor"]

WORKDIR "/opt/processor"

RUN ["mvn", "clean", "package"]

ENTRYPOINT ["java", "-jar", "/opt/processor/target/processor-1.0.jar"]

