#!/usr/bin/env sh
 protoc \
  --rust_out=. \
  --grpc_out=. \
  --plugin=protoc-gen-grpc=`which grpc_rust_plugin` \
  --proto_path=$GOPATH/src/github.com/gogo/protobuf:$GOPATH/src/github.com/prometheus/prometheus/prompb:$GOPATH/src/github.com/gogo/protobuf/protobuf/google/protobuf:/usr/local/include \
  remote.proto

