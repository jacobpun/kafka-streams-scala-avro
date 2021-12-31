$CONFLUENT_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic invoice-topic --partitions 5 --replication-factor 3 --config segment.bytes=1000000
$CONFLUENT_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic shipment-topic --partitions 5 --replication-factor 3 --config segment.bytes=1000000
$CONFLUENT_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic notification-topic --partitions 5 --replication-factor 3 --config segment.bytes=1000000
