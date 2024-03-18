FROM python:3.12

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

WORKDIR /app

RUN python3 -m pip install --upgrade pip

COPY requirements.txt ./

RUN pip install -r requirements.txt --no-cache

COPY .ENV .env

COPY . .

EXPOSE 8080

CMD cd src/wave_havoc && python wave_havoc.py
