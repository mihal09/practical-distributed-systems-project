FROM debian:bullseye-slim

RUN apt-get update && \
	apt-get install -y --no-install-recommends openjdk-11-jre-headless maven

COPY ["processor/", "/opt/app_server"]

WORKDIR "/opt/app_server"

RUN ["mvn", "clean", "package"]

ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=processor.MyApp"]

