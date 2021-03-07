# s3select
**The purpose of s3select engine** is to create an efficient pipe between user client to storage node (the engine should be as close as possible to storage, "moving computation into storage").

It enables the user to define the exact portion of data should received by his side.

It also enables for higher level analytic-applications (such as SPARK-SQL) , using that feature to improve their latency and throughput.

https://aws.amazon.com/blogs/aws/s3-glacier-select/

https://www.qubole.com/blog/amazon-s3-select-integration/

The engine is using boost::spirit to define the grammar , and by that building the AST (abstract-syntax-tree). upon statement is accepted by the grammar it create a tree of objects.

The hierarchy(levels) of the different objects also define their role, i.e. function could be a finite expression, or an argument for an expression, or an argument for other functions, and so forth.

Bellow is an example for “SQL” statement been parsed and transform into AST.
![alt text](/s3select-parse-s.png)

The where-clause is boolean expression made of arithmetic expression building blocks.

Projection is a list of arithmetic expressions

I created a container (**sudo docker run -it galsl/s3select:dev /bin/bash/**) built with boost libraries , for building and running the s3select demo application.

**The application can run on CSV files only,there is a zipped demo-input-file /s3select/datetime_decimal_float_100k.csv.gz as follow.**

* bash> /s3select/s3select/example/s3select_example -q ‘select _1 +_2,int(_5) * 3 from /...some..full-path/csv.txt where _1 > _2;’

* bash> zcat /s3select/datetime_decimal_float_100k.csv.gz | /s3select/s3select/example/s3select_example -q 'select count(*) from stdin;'

* bash> zcat /s3select/datetime_decimal_float_100k.csv.gz | head -100 | /s3select/s3select/example/s3select_example -q 'select count(*),sum(int(_2)),sum(float(_3)) from stdin;'
 
* bash> zcat /s3select/datetime_decimal_float_100k.csv.gz | /s3select/s3select/example/s3select_example -q 'select count(*) from stdin where int(_2) between 45000 and 50000;'

* bash> zcat /s3select/datetime_decimal_float_100k.csv.gz | /s3select/s3select/example/s3select_example -q 'select count(*) from stdin  where extract(year from to_timestamp(_1)) == 1933;'

-q flag is for the query.

the engine supporting the following arithmetical operations +,-,*,/,^ , ( ) , and also the logical operators and,or.

s3select is supporting float,decimal,string; it also supports aggregation functions such as max,min,sum,count; the input stream is accepted as string attributes, to operate arithmetical operation it need to CAST, i.e. int(_1) is converting text to integer.

The demo-app is producing CSV format , thus it can be piped into another s3select statement.

there is a small app /generate_rand_csv {number-of-rows} {number-of-columns}/ , which generate CSV rows containing only numbers.

the random numbers are produced with same seed number.

since it works with STDIN , it possible to concatenate several files into single stream.

cat file1 file2 file1 file2 | s3select -q ‘ ….. ‘
