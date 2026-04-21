cmake_minimum_required(VERSION 3.22)
project(RaftKVStorage)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE "Debug")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories("${PROJECT_SOURCE_DIR}/src/Common/include")
include_directories("${PROJECT_SOURCE_DIR}/src/ThreadPool/include")
include_directories("${PROJECT_SOURCE_DIR}/src/RaftServer/include")
include_directories("${PROJECT_SOURCE_DIR}/src/BlockingQueue/include")
include_directories("${PROJECT_SOURCE_DIR}/src/KVServer/include")
include_directories("${PROJECT_SOURCE_DIR}/src/KVClient/include")
include_directories("${PROJECT_SOURCE_DIR}/src/KVStore/include")

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(magic_enum CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS serialization)
find_package(spdlog REQUIRED)
find_package(yaml-cpp REQUIRED)

add_subdirectory(src)
add_subdirectory(test)

