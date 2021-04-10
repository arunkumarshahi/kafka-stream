
=============start producer =====================
docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic avro-movies --bootstrap-server broker:9092 --property value.schema="{\"namespace\":\"ak.kafka.stream.avro\",\"type\":\"record\",\"name\":\"Movie\",\"fields\":[{\"name\":\"movie_id\",\"type\":\"long\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"release_year\",\"type\":\"int\"}]}"


{"movie_id":101,"title":"Secret Weapon","release_year":1992}
{"movie_id":20,"title":"Die Hard","release_year":1988}
{"movie_id":30,"title":"Predator","release_year":1987}
{"movie_id":1280,"title":"x The Big Lebowski","release_year":1998}
{"movie_id":3540,"title":"Tree of Life","release_year":2011}
{"movie_id":7820,"title":"A Walk in the Clouds","release_year":1995}


============== start consumer ==============

docker exec -i schema-registry /usr/bin/kafka-protobuf-console-consumer --bootstrap-server broker:9092 --topic proto-movies --from-beginning


docker inspect --format='{{.Id}} {{.Name}} {{.Image}}' $(docker ps -aq)

SET OPERATIONS_COUNT=docker inspect --format='{{.Id}} {{.Name}} {{.Image}}' $(docker ps -aq)
  
  set location =$(docker ps -aqf "name=kafka-stream_kafka-stream_1")
  
  COUNT=$(docker ps -a | grep "$CONTAINER_NAME" | wc -l)
  
 
 
docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic avro-users --bootstrap-server broker:9092 --property value.schema="{\"namespace\":\"ak.kafka.stream.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":\"int\"},{\"name\":\"favorite_color\",\"type\":\"string\"}]}"


docker exec -i schema-registry /usr/bin/kafka-avro-console-consumer --bootstrap-server broker:9092 --topic avro-colors --from-beginning



{"name":"arun","favorite_number":9,"favorite_color":"RED"}
{"name":"xarun","favorite_number":9,"favorite_color":"blue"}
 
 