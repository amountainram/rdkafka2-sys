cmake_minimum_required(VERSION 3.10.0)
project(librdkafka-playground VERSION 0.0.0 LANGUAGES C)

set(C_STANDARD 11)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

add_subdirectory(librdkafka)
add_subdirectory(playground)
