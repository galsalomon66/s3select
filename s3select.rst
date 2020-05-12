===============
 Ceph s3 select 
===============

.. contents::

Overview
--------

The purpose of s3select engine is to create an efficient pipe between user client to storage node (the engine should be close as possible to storage).
It enables the user to define the exact portion of data should be received by his side.
It also enables for higher level analytic-applications (such as SPARK-SQL) , using that feature to improve their latency and throughput.
For example, a s3-object of several GB (CSV file), a user needs to extract a single column which filters by another column.
such as:
select customer-id where age>30 and age<65
currently the whole s3-object must retrieve from OSD via RGW before filtering and extracting data.
by "pushing down" the query into OSD , it's possible to save a lot of network and CPU(serial / de-serial).
The bigger the object, and the more accurate the query, the better the performance.

Features Support
----------------


The following table describes the support for s3-select functionalities:

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Detailed        | Example                                                               |
+=================================+=================+=======================================================================+
| Arithmetic operators            | ^ * / + - ( )   | select (int(_1)+int(_2))*int(_9) from stdin;                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |                 | select ((1+2)*3.14) ^ 2 from stdin;                                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Compare operators               | > < >= <= == != | select _1,_2 from stdin where (int(1)+int(_3))>int(_5);               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | AND OR          | select count(*) from stdin where int(1)>123 and int(_5)<200;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | int(expression) | select int(_1),int( 1.2 + 3.4) from stdin;                            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |float(expression)|                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | timestamp(...)  | select timestamp("1999:10:10-12:23:44") from stdin;                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | sum             | select sum(int(_1)) from stdin;                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | min             | select min( int(_1) * int(_5) ) from stdin;                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | max             | select max(float(_1)),min(int(_5)) from stdin;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | count           | select count(*) from stdin where (int(1)+int(_3))>int(_5);            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | extract         | select count(*) from stdin where                                      |
|                                 |                 | extract("year",timestamp(_2)) > 1950                                  |    
|                                 |                 | and extract("year",timestamp(_1)) < 1960;                             |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | dateadd         | select count(0) from stdin where                                      |
|                                 |                 | datediff("year",timestamp(_1),dateadd("day",366,timestamp(_1))) == 1; |  
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | datediff        | select count(0) from stdin where                                      |  
|                                 |                 | datediff("month",timestamp(_1),timestamp(_2))) == 2;                  | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | utcnow          | select count(0) from stdin where                                      |
|                                 |                 | datediff("hours",utcnow(),dateadd("day",1,utcnow())) == 24 ;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | substr          | select count(0) from stdin where                                      |
|                                 |                 | int(substr(_1,1,4))>1950 and int(substr(_1,1,4))<1960;                |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| alias support                   |                 |  select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3                  | 
|                                 |                 |  from stdin where a3>100 and a3<300;                                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+

Sending Query to RGW
====================


Syntax
~~~~~~
CSV default defintion for field-delimiter,row-delimiter,quote-char,escape-char are: { , \\n " \\ }

::

 aws --endpoint-url http://localhost:8000 s3api select-object-content 
  --bucket {BUCKET-NAME}  
  --expression-type 'SQL'     
  --input-serialization 
  '{"CSV": {"FieldDelimiter": "," , "QuoteCharacter": "\"" , "RecordDelimiter" : "\n" , "QuoteEscapeCharacter" : "\\" , "FileHeaderInfo": "USE" }, "CompressionType": "NONE"}' 
  --output-serialization '{"CSV": {}}' 
  --key {OBJECT-NAME} 
  --expression "select count(0) from stdin where int(_1)<10;" output.csv

CSV parsing behavior
====================

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Description     | input ==> tokens                                                      |
+=================================+=================+=======================================================================+
|     NULL                        | successive      | ,,1,,2,    ==> {null}{null}{1}{null}{2}{null}                         |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     QUOTE                       | quote character | 11,22,"a,b,c,d",last ==> {11}{22}{"a,b,c,d"}{last}                    |
|                                 | overrides       |                                                                       |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     Escape                      | escape char     | 11,22,str=\\"abcd\\"\\,str2=\\"123\\",last                            |
|                                 | overrides       | ==> {11}{22}{str="abcd",str2="123"}{last}                             |
|                                 | meta-character. |                                                                       |
|                                 | escape removed  |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     row delimiter               | no close quote, | 11,22,a="str,44,55,66                                                 |
|                                 | row delimiter is| ==> {11}{22}{a="str,44,55,66}                                         |
|                                 | closing line    |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     csv header info             | FileHeaderInfo  | for "USE" value, each token on first line is column-name              |
|                                 | tag             | "IGNORE" value, means to skip the first line                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
