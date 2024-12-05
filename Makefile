proto:
	@protoc --go_out=./hdfs-api/protos --go-grpc_out=./hdfs-api/protos --proto_path=./hdfs-api/protos --proto_path=./vendor/protos/src ./hdfs-api/protos/hdfs.proto

esdb:
	@docker run --name esdb-node -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:lts --insecure --run-projections=All --start-standard-projections=true --enable-atom-pub-over-http