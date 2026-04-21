
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SRC_CLIENT_TEST)

add_executable(kv_client_test ${SRC_CLIENT_TEST})
target_link_libraries(kv_client_test KVClient magic_enum::magic_enum)
