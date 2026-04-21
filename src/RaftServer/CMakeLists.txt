

get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

# 将该目录下所有源文件添加到 SRC_LIST 变量中
aux_source_directory(${SRC_DIR} src_raft)

add_library(RaftServer STATIC ${src_raft})
# target_link_libraries(RaftServer PUBLIC protos_lib protobuf::libprotobuf threadpool
#         magic_enum::magic_enum Tracy::TracyClient Boost::serialization spdlog::spdlog)

target_link_libraries(RaftServer PUBLIC protos_lib protobuf::libprotobuf threadpool
        magic_enum::magic_enum Boost::serialization spdlog::spdlog)

if(ENABLE_TRACY)
        target_link_libraries(RaftServer PUBLIC Tracy::TracyClient)
endif()

#target_compile_options(RaftServer PRIVATE
#        $<$<CONFIG:Debug>:-fsanitize=address;-fno-omit-frame-pointer>
#)
#target_link_options(RaftServer PRIVATE
#        $<$<CONFIG:Debug>:-fsanitize=address>
#)