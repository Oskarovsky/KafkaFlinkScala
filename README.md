### Odpalanie KAFKI:

1. Zakładka ZOOKEEPER -->  /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
2. Zakładka KAFKA    -->  /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
3. Zakładka MANAGER
	tworzenie topicu1 --> /opt/kafka/bin/kafka-topics.sh --create --topic temat_bus01 --bootstrap-server localhost:9092
	tworzenie topicu2 --> /opt/kafka/bin/kafka-topics.sh --create --topic temat_tram01 --bootstrap-server localhost:9092
	sprawdzanie stanu --> /opt/kafka/bin/kafka-topics.sh --describe --topic temat_bus01 --bootstrap-server localhost:9092
	
4. Zakładka CONSUMER TOPIC1   -->  /opt/kafka/bin/kafka-console-consumer.sh --topic temat_bus01 --bootstrap-server localhost:9092
5. Zakładka PRODUCENT TOPIC1  -->  /opt/kafka/bin/kafka-console-producer.sh --topic temat_bus01 --bootstrap-server localhost:9092

6. Zakładka CONSUMER TOPIC2   -->  /opt/kafka/bin/kafka-console-consumer.sh --topic temat_tram01 --bootstrap-server localhost:9092
7. Zakładka PRODUCENT TOPIC2  -->  /opt/kafka/bin/kafka-console-producer.sh --topic temat_tram01 --bootstrap-server localhost:9092
