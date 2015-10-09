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

Zookeeper:
----------

Create a symbolic link to the loggo bigjar, which will provide the UDP
Appender. If you are running log4j2, you could use the Appender in
that release. Since zookeeper 3.4.6 still uses log4j 1.2, these
configurations use the one in loggo-bigjar:

	ln -s /some/location/loggo-bigjar.jar lib

Change the log4j.properties file:

	log4j.rootLogger=${zookeeper.root.logger},UDP

	log4j.appender.UDP=org.loggo.client.UDPAppender
	# replace this with one of the hosts running the kafka consumer
	log4j.appender.UDP.remoteHost=loggo-server-host
	log4j.appender.UDP.port=9991
	log4j.appender.UDP.application=zookeeper
	log4j.appender.UDP.layout=org.apache.log4j.EnhancedPatternLayout
	log4j.appender.UDP.layout.ConversionPattern=%properties{hostname} %properties{application} %d{ISO8601} [%c] %p: %m

Once zookeeper is running, go ahead and start kafka.

Kafka Broker:
-------------
The "broker.id" of each kafka server needs to be different.
Remember to expand the zookeeper list on larger systems.

Since zookeeper is being used by multiple applications,
it's a good idea to configure it to use the "chrooted"
zookeeper settings. Be sure to make the location before
starting kafka:

	$ echo 'create /kafka ""' | zkCli.sh -server zoohost:2181

After kafka is configured with the appropriate zookeeper config,
create the topic for logs.  By default this topic is "logs" and can be
created like this:

	$ ./bin/kafka-topics.sh --zookeeper host1:2181,host2:2181,host3:2181/kafka --create --topic logs --partitions 1 --replication 1

This only needs to be done once, and as noted lated, it will be done
for you if you forget. However, it may prevent logs from being sent by
other services before the loggo server is started.

Hadoop:
-------

Add the following lines to hadoop-env.sh, which will allow the log4j
configuration to know the current host and application:

	export HADOOP_OPTS="${HADOOP_OPTS} -Dhadoop.hostname=$(hostname) -Dhadoop.application=$1 "

Edit the log4j.properties file. Change the rootLogger line from:

	log4j.rootLogger=${hadoop.root.logger}, EventCounter

to:
	log4j.rootLogger=${hadoop.root.logger}, EventCounter, KAFKA

Add the KAFKA appender:

	log4j.appender.KAFKA=kafka.producer.KafkaLog4jAppender
	log4j.appender.KAFKA.topic=logs
	log4j.appender.KAFKA.threshold=DEBUG
	# edit this line to point to your kafka hosts:
	log4j.appender.KAFKA.brokerList=kafka-host:9092,kafka-host2:9092
	log4j.appender.KAFKA.layout=org.apache.log4j.EnhancedPatternLayout
	log4j.appender.KAFKA.layout.ConversionPattern=${hadoop.hostname} ${hadoop.application} %d{ISO8601} [%c] %p: %m

Create a symbolic link to the loggo bigjar, which will pull in kafka,
scala and other necessary libraries to support the appender:

	ln -s /some/location/loggo-bigjar.jar share/hadoop/common/lib

Note that kafka needs to be running on the kafka hosts to begin accepting
the log messages.  Remember to make these changes to all the hosts 
running hadoop.

Accumulo:
---------

Use accumulo to run the kafka consumer.  This requires a few more
symlinks:

	# link in all the loggo jars
	ln -s /some/location/loggo*.jar lib

	# add in netty
	ln -s /some/location/netty-all-4.0*.jar lib

Edit the conf/generic_logger.xml file.  Add the kafka appender:

	<!-- Send all logging data kafka -->
	<appender name="KAFKA" class="kafka.producer.KafkaLog4jAppender">
	  <!-- ====== Edit this ======-->
	  <param name="brokerList"   value="kafka-host:9092"/>
	  <param name="topic"        value="logs"/>
	  <param name="Threshold"      value="DEBUG"/>
	  <layout class="org.apache.log4j.EnhancedPatternLayout">
	    <param name="ConversionPattern" value="${org.apache.accumulo.core.ip.localhost.hostname} ${org.apache.accumulo.core.application} %d{ISO8601} [%-8c{2}] %-5p: %m"/>
	  </layout>
	</appender>

Remember to use your kafka host(s). Turn off lower-level logging in
the kafka appender so it doesn't deadlock:

	<logger name="org.apache.kafka">
	  <level value="ERROR"/>
	</logger>
	
	<logger name="kafka">
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

It is *not* recommended that you use the KAFKA appender for the
log4j.properties file for accumulo.  For the few applications that
will use this file (like the accumulo shell), use the UDP appender
instead. See the zookeeper instructions.

Add the following to the accumulo-env.sh file, to pick up the new jars
before the path extension code runs:

	CLASSPATH="${CLASSPATH}:$(echo ${ACCUMULO_HOME}/lib/loggo-bigjar.jar)"
	CLASSPATH="${CLASSPATH}:$(echo ${ZOOKEEPER_HOME}/zookeeper*.jar)"
	CLASSPATH="${CLASSPATH}:$(echo ${ACCUMULO_HOME}/lib/netty*.jar)"


System logs
-----------

Logstash can be used to parse system logs and forward them to loggo.
The following logstash configuration will read messages from
/var/log/messages:

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
	     code => "event['time'] = event.sprintf('%{+YYYY-MM-dd HH:mm:ss,SSS}')"
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

The kafka consumer that injects log entries into Accumulo can be run
from the accumulo directory. Link or copy the loggo jar files into the
accumulo lib directory.  You can create a loggo server config file
from the defaults file, and then run the server:

	$ ln -s /some/path/somewhere/loggo/lib/*.jar lib
	$ cp /some/path/somewhere/loggo/conf/loggo-server.ini.defaults conf/loggo-server.ini
	$ $EDITOR conf/loggo-server.ini
	$ ./bin/accumulo loggo-server --config conf/loggo-server.ini

Note that during the first run, if the configured table does not
exist, it will be created and configured with several initial
split-points and a 31-day age off.  If you want to run the
loggo-server with a different accumulo login, create the user and
grant them Table.CREATE priviledges.  Also during the first run, the
kafka topic will be created, if it does not exist. Services that are
started before the topic is created will not log to kafka until they
restarted.

The loggo server can be run on multiple machines, as can the kafka
message service nodes.  This provides redundancy and scaling of log
collection.  Just be certain to add all your servers to the brokerList
settings.

The default latency for writting to accumulo is several minutes.
You will want to decrease this timeout for initial installations
so the logs will appear in the table quickly after being received.
