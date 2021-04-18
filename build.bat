call cls 
call mvn clean install 
call docker build -t arunkumarshahi/kafka-stream .  
call docker-compose up -d 
call docker ps

call docker ps -aqf "name=kafka-stream_kafka-stream_1"

rem call C:\DockerTutorial\spring\kafka-stream>docker exec -i schema-registry /usr/bin/kafka-avro-console-consumer --bootstrap-server broker:9092 --topic avro-colors --from-beginning

docker exec -i schema-registry /usr/bin/kafka-avro-console-consumer --bootstrap-server broker:9092 --topic avro-sum  --from-beginning --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer


