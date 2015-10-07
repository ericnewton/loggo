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

How to configure various hadoop components to write log data to loggo
======

Kafka:
------
The "broker.id" of each kafka server needs to be different, but due to [KAFKA-2152][1], at least one server needs to have broker.id=0. Remember to expand the zookeeper list on larger systems.

Kafka needs zookeeper to run.

Zookeeper:
----------

Create a symbolic link to the loggo bigjar, which will provide the UDP Appender. If you are running log4j2, you could use the Appender in that release. Since zookeeper 3.4.6 still uses log4j 1.2, these configurations use the one in loggo-bigjar:

	ln -s /some/location/loggo-bigjar.jar lib

Change the log4j.properties file:

	log4j.rootLogger=WARN, CONSOLE ,UDP
	log4j.logger=INFO

	log4j.appender.UDP=org.loggo.client.UDPAppender
	log4j.appender.UDP.remoteHost=loggo-server-host
	log4j.appender.UDP.port=9991
	log4j.appender.UDP.application=zookeeper
	log4j.appender.UDP.layout=org.apache.log4j.EnhancedPatternLayout
	log4j.appender.UDP.layout.ConversionPattern=%properties{hostname} %properties{application} %d{ISO8601} [%c] %p: %m

Once zookeeper is running, go ahead and start kafka.


Hadoop:
-------

Add the following lines to hadoop-env.sh:

	export HADDOP_OPTS="${HADOOP_OPTS} -Dhadoop.hostname=$(hostname) -Dhadoop.application=$1 "
	export HADOOP_ROOT_LOGGER=WARN,RFA

Edit the log4j.properties file. Change the rootLogger line from:
	log4j.rootLogger=${hadoop.root.logger}, EventCounter

to:
	log4j.rootLogger=${hadoop.root.logger}, EventCounter, KAFKA
	log4j.logger=INFO

Add the KAFKA appender:

	log4j.appender.KAFKA=kafka.producer.KafkaLog4jAppender
	log4j.appender.KAFKA.topic=logs
	log4j.appender.KAFKA.brokerList=kafka-host:9092
	log4j.appender.KAFKA.layout=org.apache.log4j.EnhancedPatternLayout
	log4j.appender.KAFKA.layout.ConversionPattern=${hadoop.hostname} ${hadoop.application} %d{ISO8601} [%c] %p: %m

Create a symbolic link to the loggo bigjar, which will pull in kafka, scala and other necessary libraries to 
support the appender:

	ln -s /some/location/loggo-bigjar.jar share/hadoop/common/lib

Note that kafka needs to be running on "kafka-host" to being accepting the log messages.

Accumulo:
---------

Use accumulo to run the kafka consumer.  This requires a few more symlinks:

	# link in all the loggo jars
	ln -s /some/location/loggo*.jar lib

	# add in netty
	ln -s /some/location/netty-all-4.0*.jar lib

Edit the conf/generic_logger.xml file.  Add the kafka appender:

	<!-- Send all logging data kafka -->
	<appender name="KAFKA" class="kafka.producer.KafkaLog4jAppender">
	  <param name="brokerList"   value="kafka-host:9092"/>
	  <param name="topic"        value="logs"/>
	  <param name="Threshold"      value="DEBUG"/>
	  <layout class="org.apache.log4j.EnhancedPatternLayout">
	    <param name="ConversionPattern" value="${org.apache.accumulo.core.ip.localhost.hostname} ${org.apache.accumulo.core.application} %d{ISO8601} [%-8c{2}] %-5p: %m"/>
	  </layout>
	</appender>

Remember to use your kafka host(s). Turn off lower-level logging in the kafka appender so it doesn't deadlock:

	<logger name="org.apache.kafka">
	  <level value="ERROR"/>
	</logger>

Add the kafka appender:

	<!-- Log non-accumulo events to the debug and normal logs. -->
	<root>
	  <level value="DEBUG"/>
	  <appender-ref ref="A2" />
	  <appender-ref ref="A3" />
	  <appender-ref ref="N1" />
	  <appender-ref ref="KAFKA" />
	</root>

It is not recommended that you use the KAFKA appender for the log4j.properties file for accumulo.  For the few applications that will use this file (like the accumulo shell), use the UDP appender instead. See the zookeeper instructions.

Add the following to the accumulo-env.sh file, to pick up the new jars before the path extension code runs:

ACCUMULO_GENERAL_OPTS="${ACCUMULO_GENERAL_OPTS} -Dhostname=$(hostname) "
CLASSPATH="${CLASSPATH}:$(echo ${ACCUMULO_HOME}/lib/loggo-bigjar.jar)"
CLASSPATH="${CLASSPATH}:$(echo ${ZOOKEEPER_HOME}/zookeeper*.jar)"
CLASSPATH="${CLASSPATH}:$(echo ${ACCUMULO_HOME}/lib/netty*.jar)"


System logs
-----------

Logstash can be used to parse system logs and forward them to loggo.  The following logstash configuration will
read messages from /var/log/messages:

	input { 
	  file { path => '/var/log/messages' }
	}
	filter {
	  grok {
	    match => ["message", "%{SYSLOGBASE} %{GREEDYDATA:syslog_message}" ]
	  }
	  date { 
	    match => [ "timestamp", "MMM dd HH:mm:ss", "MMM  d HH:mm:ss"]
	  }
	  ruby {
	     code => "
	        event['time'] = event.sprintf('%{+YYYY-MM-dd HH:mm:ss,SSS}')
	     "
	    }
	}
	output { 
	  kafka { 
	    topic_id => 'logs' 
	    broker_list => 'kafka-host:9092'
	    codec => line { 
	      format => "%{host} %{program} %{time} %{syslog_message}" 
	    }
	  }
	}

Loggo Server:
-------------

The kafka consumer that injects log entries into Accumulo can be run from the accumulo directory.  Once all the 
loggo jar files and netty jar file have been linked or copied into the accumulo lib directory, You can run the
loggo server:

	$ ./bin/accumulo loggo-server --config conf/loggo-server.ini

The loggo conf/loggo-server.ini file can serve as a starting point for creating your own configuration, and the
options are documented in that file.

Note that during the first run, if the configured table does not exist, it will be created and configured with
several initial split-points.  If you want to run the loggo-server with a different accumulo login, create the
user and grant them Table.CREATE priviledges.  Also during the first run, the kafka topic will be created, if it
does not exist. Services that are started before the topic is created will not log to kafka until they restarted.

The loggo server can be run on multiple machines, as can the kafka message service nodes.  This provides redundancy
and scaling of log collection.  Just be certain to add all your servers to the brokerList settings.

[1]: https://issues.apache.org/jira/browse/KAFKA-2152
