
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SRC_KV_CLIENT)

add_library(KVClient ${SRC_KV_CLIENT})
target_link_libraries(KVClient protos_lib)