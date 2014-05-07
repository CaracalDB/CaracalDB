# CaracalDB

[kompics]: https://github.com/kompics/kompics
[leveldb]: https://code.google.com/p/leveldb/

A consistent, scalable and flexible distributed key-value store.

CaracalDB provides the following features/properties:
* It uses a flat key-space of byte-sequences which are stored in prefix order and are not hashed.
* It has support for range queries over keys.
* It automatically partitions the key-space during runtime and distributes it over a large number of virtual nodes on top of the physical hosts in the system.
* Each partition is replicated on multiple virtual nodes using a variant of the Paxos algorithm for a replicated log.
* Partitions are load balanced across the cluster based on different load metrics.
* Data can be stored either in Memory or in [LevelDB][leveldb].

CaracalDB is written in Java using the [Kompics][kompics] component framework.

CaracalDB provides the following clients:
* A (non-)blocking Java client for direct inclusion in application server software
* A HTTP/JSON based RESTful client written in Scala
* A shell-like command-line client

## Getting Started

### Downloading

You can find pre-built jars and packages at http://cloud7.sics.se/caracal/.

### Building

You need Oracle JDK 8 and Maven 3.

Checkout the code from Github and build with
```sh
mvn clean install
```
Most likely you will also want a "fat" jar for deployment:
```sh
cd server
mvn assembly:assembly
```
This will create a "fat" jar for the server in `server/target`. 
And equivalent procedure applies to the client, but is not generally recommended. It is better to pull dependencies via maven when including the client code in application server software.

To build the REST API you will need SBT:
```sh
cd rest-api
sbt assembly
```

### Running
You can run a single instance of the CaracalDB server with a simple start script like
```sh
#!/bin/bash
LFJ={path to log4j.properties file}
APPC={path to application.conf}
SIG_LIBS={path to sigar native libs}
nohup java -Dlog4j.configuration=file://$LFJ -Dconfig.file=$APPC -Djava.library.path=$SIG_LIBS -jar caracaldb-server.jar &> nohup.out &
eval 'echo $!' >> caracal.pid
```
Please note, that depending on your configuration you will need at least 3 instances of the CaracalDB running before the system actually "boots up".
Examples for the configuration files can be found in the [Reference-Conf](server/src/main/resources/reference.conf) and [Log4J-Conf](server/src/main/resources/log4j.properties).

You will have to run the REST API separately from the actual servers. You can start it from sbt with
```sh
sbt re-start
```
or use a start script similar to the one above for the assembled version.

### Java API
Once you have a working cluster running you can use the Java API to execute operations.
A simple example for all the core operations would be:
```java
import se.sics.caracaldb.Key;
import se.sics.caracaldb.client.BlockingClient;
import se.sics.caracaldb.client.ClientManager;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.ResponseCode;

class SomeClass {
	public boolean someMethod(Key k, byte[] value) {
		BlockingClient client = ClientManager.newClient();
		ResponseCode putResp = client.put(k, val);
		if (putResp != ResponseCode.SUCCESS) {
            		return false;
        		}
		GetResponse getResp = client.get(k);
		if (getResp.code != ResponseCode.SUCCESS) {
            		return false;
        		}
		KeyRange range = KeyRange.closed(k).closed(k);
		RangeResponse rangeResp = client.rangeRequest(range);
		if (rangeResp.code != ResponseCode.SUCCESS) {
            		return false;
        		}
		return Arrays.equal(value, getResp.data) 
			&& rangeResp.results.contains(k);
	}
}
```
For more advanced usage feel free to look into the implementation of the [command-line client](client/src/main/java/se/sics/caracaldb/client/Console.java) and the [REST API](rest-api/src/main/scala/se/sics/caracaldb/api/CaracalWorker.scala).

### REST API
CaracalDB's REST API provides the following services:
* *PUT/GET/DELETE* at `/schema/{schema}/key/{key}` on key in schema. If type is *PUT* then the body of the request must be a string (or a JSON object as a string) of the value to be put.
* *GET* at `/schema/{schema}/key/{fromkey}/{tokey}` performs a range query of the range \[fromkey,tokey\] (prefixed by schema).
* *GET* at `/schema/{schema}/prefix/{key}` performs a range query of all entries with the prefix key in schema.
* *GET* at `/schema/{schema}` performs a range query of all entries with in schema.

All requests are responded to in a JSON format.


## License

CaracalDB is under the GNU GENERAL PUBLIC LICENSE, Version 2. See the [LICENSE](LICENSE) file for details.
