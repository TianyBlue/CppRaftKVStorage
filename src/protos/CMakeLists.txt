
file(GLOB PROTO_FILES LIST_DIRECTORIES false
        "${CMAKE_CURRENT_SOURCE_DIR}/*.proto"
)

message("Gathered protos: ${PROTO_FILES}")

protobuf_generate(
        LANGUAGE cpp
        PROTOS ${PROTO_FILES}
        IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"  # 用于 import 其他 .proto
#        OUTPUT_DIR "${PROTO_OUTPUT_DIR}"
        OUT_VAR PROTO_PB_SRCS
)

# 生成 gRPC 服务代码 (.grpc.pb.cc / .grpc.pb.h)
protobuf_generate(
        LANGUAGE grpc
        PROTOS ${PROTO_FILES}
        GENERATE_EXTENSIONS .grpc.pb.cc .grpc.pb.h
        IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}"
        OUT_VAR PROTO_GRPC_SRCS
)

# 合并源文件
set(PROTO_SRCS ${PROTO_PB_SRCS} ${PROTO_GRPC_SRCS}
        ../Common/defer_class.cpp
        ../Common/include/defer_class.h)

add_library(protos_lib STATIC ${PROTO_SRCS})

target_include_directories(protos_lib PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)

set_target_properties(protos_lib PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}"
)

target_link_libraries(protos_lib PUBLIC protobuf::libprotobuf gRPC::grpc++)
