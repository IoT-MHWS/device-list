#!/bin/bash

ROOT_DIR=.
GEN_DIR=${ROOT_DIR}/_gen
PROTO_DIR=${ROOT_DIR}/common/proto

mkdir -p ${GEN_DIR}

find -L ${PROTO_DIR} -type f -name "*.proto" | while read -r file; do
  python -m grpc_tools.protoc -I ${PROTO_DIR} --python_out=${GEN_DIR} --pyi_out=${GEN_DIR} --grpc_python_out=${GEN_DIR} ${file}
done
