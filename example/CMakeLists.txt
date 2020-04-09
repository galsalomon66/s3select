add_executable(s3select_example s3select_example.cpp)
target_include_directories(s3select_example PUBLIC ../include)
target_link_libraries(s3select_example boost_date_time)

add_executable(generate_rand_csv generate_rand_csv.c)

