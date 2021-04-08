
=============start producer =====================
docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --topic avro-movies --bootstrap-server broker:9092 --property value.schema="{\"namespace\": \"ak.kafka.stream.avro\",\"type\": \"record\",\"name\": \"Movie\",\"fields\": [  {    \"name\": \"movie_id\",    \"type\": \"long\"  },  {    \"name\": \"title\",    \"type\": \"string\"  },  {    \"name\": \"release_year\",    \"type\": \"int\"  }]}"


{"movie_id":1,"title":"Lethal Weapon","release_year":1992}
{"movie_id":2,"title":"Die Hard","release_year":1988}
{"movie_id":3,"title":"Predator","release_year":1987}
{"movie_id":128,"title":"The Big Lebowski","release_year":1998}
{"movie_id":354,"title":"Tree of Life","release_year":2011}
{"movie_id":782,"title":"A Walk in the Clouds","release_year":1995}


============== start consumer ==============

docker exec -i schema-registry /usr/bin/kafka-protobuf-console-consumer --bootstrap-server broker:9092 --topic proto-movies --from-beginning


  