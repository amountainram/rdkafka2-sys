set(CMAKE_C_FLAGS "-Wall -Wextra -pedantic")

add_executable(simple_producer producer/main.c)
target_include_directories(simple_producer PRIVATE librdkafka/src)
target_link_libraries(simple_producer PRIVATE rdkafka)