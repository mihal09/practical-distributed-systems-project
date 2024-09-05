FROM debian:bullseye-slim

RUN apt-get update && \
	apt-get install -y --no-install-recommends python3 python3-pip wget

WORKDIR "/opt/kafka"

COPY ["main_server/requirements.txt", "/tmp/requirements.txt"]

RUN ["pip3", "install", "-r", "/tmp/requirements.txt"]

COPY ["main_server/", "/opt/app_server"]

WORKDIR "/opt/app_server"

#ENTRYPOINT ["flask", "--app", "main_new", "run", "-h", "0.0.0.0", "-p", "5000", "--with-threads", "--no-debugger"]

#ENTRYPOINT ["fastapi", "run", "--host", "0.0.0.0", "--port", "5000", "main.py"]

#ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000", "--log-level", "critical"]

#ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:5000", "--log-level", "critical", "-w", "3", "-k", "uvicorn.workers.UvicornWorker", "main:app"]

ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:5000", "-w", "3", "-k", "uvicorn.workers.UvicornWorker", "main:app"]


