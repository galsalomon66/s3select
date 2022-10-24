# s3select 
<br />The s3select is another S3 request, that enables the client to push down an SQL statement(according to [spec](https://docs.ceph.com/en/latest/radosgw/s3select/#features-support)) into CEPH storage.
<br />The s3select is an implementation of a push-down paradigm.
<br />The push-down paradigm is about moving(“pushing”) the operation close to the data.
<br />It's contrary to what is commonly done, i.e. moving the data to the “place” of operation.
<br />In a big-data ecosystem, it makes a big difference. 
<br />In order to execute __“select sum( x + y) from s3object where a + b > c”__ 
<br />It needs to fetch the entire object to the client side, and only then execute the operation with an analytic application,
<br />With push-down(s3-select) the entire operation is executed on the server side, and only the result is returned to the client side.


## Analyzing huge amount of cold/warm data without moving or converting 
<br />The s3-storage is reliable, efficient, cheap, and already contains a huge amount of objects, It contains many CSV, JSON, and Parquet objects, and these objects contain a huge amount of data to analyze.
<br />An ETL may convert these objects into Parquet and then run queries on these converted objects or may run these queries without converting.
<br />But it comes with an expensive price, downloading all of these objects close to the analytic application.

<br />The s3select-engine that resides on s3-storage can do these jobs for many use cases, saving time and resources. 


## The s3select engine stands by itself 
<br />The engine resides on a dedicated GitHub repo, and it is also capable to execute SQL statements on standard input or files residing on a local file system.
<br />Users may clone and build this repo, and execute various SQL statements as CLI.

## A docker image containing a development environment
An immediate way for a quick start is available using the following container.
That container already contains the cloned repo, enabling code review and modification.
### Running the s3select container image
`sudo docker run -w /s3select -it galsl/ubunto_arrow_parquet_s3select:dev`
### Running google test suite, it contains hundreads of queries
`./test/s3select_test`
### Running SQL statements using CLI on standard input
`./example/s3select_example`, is a small demo app, it lets you run queries on local file or standard input.
for one example, the following runs the engine on standard input.
<br />`seq 1 1000 | ./example/s3select_example -q 'select count(0) from stdin;'`
### Running SQL statements using CLI on local file
it possible to run a query on local file, as follows.
<br />`./example/s3select_example -q 'select count(0) from /full/path/file_name;'`

