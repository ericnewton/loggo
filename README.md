<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

logjam: a scalable live log collection service and tools.

Why some new log collection service?

1. scalable: this service will allow many clients to send data to many servers
1. logging clients can be written in any language
1. built to answer simple distributed systems questions: what happened between 2AM and 3AM last night?
1. must handle a million events / second
	* 10 messages / second
	* for 10 services / node
	* for 10K nodes

The simplest logger can be very simple.  They just need to connect to a socket and send a properly formatted message. The format is a line containing hostname<space>application<space>message. For example:

   $ NOW=$(date '+%y-%m-%d %H:%M:%S,000')
   $ echo $(hostname) echoApp ${NOW} 'This is a log message.\n' | nc loggerhost 9991

You can use UDP messages if there's any concern about the availability of the service interfering with the application:

	$ echo $(hostname) echoApp ${NOW} 'This is a log message.' | nc -u loggerhost 9991

Using TCP connections, the messages are terminated with double-newlines, like the example above.

In many large hadoop installations, logging can be controlled using log4j Appenders. The most reliable and scalable means for sending log messages uses Apache Kafka.

By adding:

    HADOOP_OPTS="${HADOOP_OPTS} -Dhadoop.hostname=$(hostname) -Dhadoop.application=$1 "

to <pre>$HADOOP_CONF_DIR/hadoop-env.sh</pre> and configuring a log4j appender in log4j.property: 

	log4j.appender.KAFKA=kafka.producer.KafkaLog4jAppender
	log4j.appender.KAFKA.topic=logs
	log4j.appender.KAFKA.brokerList=kafkahost1:9092,kafkahost2:9092
	log4j.appender.KAFKA.layout=org.apache.log4j.EnhancedPatternLayout
	log4j.appender.KAFKA.layout.ConversionPattern=${hadoop.hostname} ${hadoop.application} %d{ISO8601} [%c] %p: %m

Kafka will deliver log messages to a service which will store the messages in Accumulo, though other back-ends would not be difficult to write.

There is a simple command line search utility which uses the power of Accumulo iterators to distribute the data extraction for analysis.

For example:

	# count the log messages from r002n05 from the application "datanode"
	$ ./bin/accumulo logjam-search -h r002n05 -a datanode --count
	11230405

	# find all the logs from r101n07 from today
	$ ./bin/accumulo logjam-search -h r101n07 -s today
	2015-09-01 01:02:03,123	echoApp localhost		This is a log message.
	
	# find all the logs from r00{1,2,3}n01 from zookeeper starting this month
	$ ./bin/accumulo logjam-search -h r001n01 -h r002n01 -h r002n01 -a zookeeper -s $(date +%Y-%m-01)

Simply configure your java services like hadoop, drop in the logjam-client-kafka.jar, and your services will forward their logs using a redundant, scalable service.


