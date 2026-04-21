
get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

# 将该目录下所有源文件添加到 SRC_LIST 变量中
aux_source_directory(${SRC_DIR} src_kv_server)

add_library(KVServer STATIC ${src_kv_server})
add_executable(KVServerNode ${src_kv_server})
target_link_libraries(KVServer PUBLIC RaftServer kv_store yaml-cpp::yaml-cpp)
target_link_libraries(KVServerNode PUBLIC RaftServer kv_store yaml-cpp::yaml-cpp)

