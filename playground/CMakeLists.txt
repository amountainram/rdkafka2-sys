set(CMAKE_C_FLAGS "-Wall -Wextra -pedantic")

aux_source_directory(async_producer async_producer_SOURCE_DIR)
add_executable(
    async_producer
    ${async_producer_SOURCE_DIR}
    ${logc_SOURCE_DIR}/src/log.c
)
target_include_directories(
    async_producer PRIVATE
    librdkafka/src
    ${logc_SOURCE_DIR}/src
    ${cds_SOURCE_DIR}/include
    ${cjson_SOURCE_DIR}
)
target_link_libraries(async_producer PRIVATE rdkafka PRIVATE cds PRIVATE cjson)
target_compile_options(async_producer PRIVATE "-DLOG_USE_COLOR")

aux_source_directory(admin_client admin_client_SOURCE_DIR)
add_executable(
    admin_client
    ${admin_client_SOURCE_DIR}
    ${logc_SOURCE_DIR}/src/log.c
)
target_include_directories(
    admin_client PRIVATE
    librdkafka/src
    ${logc_SOURCE_DIR}/src
)
target_link_libraries(admin_client PRIVATE rdkafka)
target_compile_options(admin_client PRIVATE "-DLOG_USE_COLOR")
