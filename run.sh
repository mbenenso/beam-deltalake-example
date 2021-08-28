#!/bin/bash

cd /Users/mbenenson/my/intuit/beam-os-deltalake-standalone-example/

#  copy dependencies
#  mvn dependency:copy-dependencies -DoutputDirectory=lib -U

java -cp  "target/beam-sdks-java-io-deltalake-example-2.29.0.jar:target/lib/config-1.3.3.jar:target/lib:target/lib/*" \
   org.apache.beam.sdk.io.deltalake.example.delta.ReadDelta01

echo "Press Enter to exit ..."
read 