cmake_minimum_required(VERSION 3.10.0)
project(librdkafka-playground VERSION 0.0.0 LANGUAGES C)

set(C_STANDARD 11)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

# deps #########################################################################
include(FetchContent)
include(ExternalProject)

# LOG_H
FetchContent_Declare(
    logc
    GIT_REPOSITORY https://github.com/rxi/log.c.git
    GIT_TAG f9ea34994bd58ed342d2245cd4110bb5c6790153
)
FetchContent_MakeAvailable(logc)

# CDS_H
FetchContent_Declare(
    cds
    GIT_REPOSITORY git@github.com:amountainram/common-data-structures.git
    GIT_TAG fix/mpsc-rx-closure
)
FetchContent_MakeAvailable(cds)

# CJSON_H
FetchContent_Declare(
    cjson
    GIT_REPOSITORY https://github.com/DaveGamble/cJSON.git
    GIT_TAG v1.7.18
)
FetchContent_MakeAvailable(cjson)
################################################################################

add_subdirectory(librdkafka)
add_subdirectory(playground)
