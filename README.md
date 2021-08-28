# Example for Reading Delta Lake data from Apache Beam

 - download Beam Delta Lake connector from [beam-deltalake](https://github.com/mbenenso/beam-deltalake)
 - run 'mvn clean install'
 - download Beam Delta Lake example from [beam-deltalake-example](https://github.com/mbenenso/beam-deltalake-example)
 - rn 'mvn clean package'
 - run 'run.sh' from the current dir. Example program will read Delata Lake data from [data/delta-lake-stream-03](data/delta-lake-stream-03)
 - look for 'LogValueFn - ->  TestEvent {id=XX, name='data-XX'' in the console output

This code has been tested with Flink runner, and Delta Lake data on local disk & AWS s3.
