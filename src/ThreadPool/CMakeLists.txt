
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} THREAD_POOL_SRC)

add_library(threadpool ${THREAD_POOL_SRC})