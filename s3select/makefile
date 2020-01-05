

all :	s3select generate_rand_csv


s3select :	s3select_test.cpp s3select_oper.h s3select.h
		g++ -I.. -O3 -ggdb s3select_test.cpp -o s3select
		./run_test.bash

generate_rand_csv : generate_rand_csv.c
		gcc -O3 generate_rand_csv.c -o generate_rand_csv

