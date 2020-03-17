cmake_minimum_required(VERSION 3.0)

project(s3select)

set(CMAKE_CXX_FLAGS "-std=gnu++17")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED)
find_package(GTest REQUIRED)

enable_testing() 

add_subdirectory(example)
add_subdirectory(test)

