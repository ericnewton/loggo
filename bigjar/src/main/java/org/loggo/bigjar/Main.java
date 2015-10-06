package org.loggo.bigjar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.KafkaLog4jAppender;

public class Main {

  public static void main(String[] args) throws Exception {
    Logger log = LoggerFactory.getLogger(Main.class);
    log.info("Hello World!");
    // verify that we're pulling in the right jar files
    @SuppressWarnings("unused")
    Class<?> klass = KafkaLog4jAppender.class;

    // wait for the message to flush to kafka
    Thread.sleep(5 * 1000);
  }

}
