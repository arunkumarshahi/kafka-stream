call cls 
call mvn clean install 
call docker build -t arunkumarshahi/kafka-stream .  
call docker-compose up -d 
call docker ps

call docker ps -aqf "name=kafka-stream_kafka-stream_1"


