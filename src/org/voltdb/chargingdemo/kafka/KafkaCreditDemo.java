package org.voltdb.chargingdemo.kafka;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaCreditDemo {

	private static final String QUOTE_COMMA_QUOTE = "\",\"";
	private static final String SINGLE_QUOTE = "\"";

	public static void main(String[] args) throws UnknownHostException {

		msg("Parameters:" + Arrays.toString(args));

		if (args.length != 5) {
			msg("Usage: kafkaserverplusport recordcount  tpms durationseconds maxamount");
			System.exit(1);
		}

		String kafkaserverplusport = args[0];
		int recordCount = 0;
		int tpms = 0;
		int durationseconds = 0;
		int maxamount = 0;
		Random r = new Random();

		try {
			recordCount = Integer.parseInt(args[1]);
			tpms = Integer.parseInt(args[2]);
			durationseconds = Integer.parseInt(args[3]);
			maxamount = Integer.parseInt(args[4]);

		} catch (NumberFormatException e) {
			msg("Value should be a number:" + e.getMessage());
			System.exit(1);
		}
		
		
		Properties config = new Properties();
		config.put("client.id", InetAddress.getLocalHost().getHostName());
		config.put("bootstrap.servers", kafkaserverplusport);
		config.put(
	              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
	              StringSerializer.class);
		config.put(
	              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
	              StringSerializer.class);
		config.put("acks", "all");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String> (config);

		long endtimeMs = System.currentTimeMillis() + (1000 * durationseconds);
		// How many transactions we've done...
		
		int tranCount = 0;

		int tpThisMs = 0;
		long currentMs = System.currentTimeMillis();
		
		while (endtimeMs > System.currentTimeMillis()) {
			

			if (tpThisMs++ > tpms) {

				while (currentMs == System.currentTimeMillis()) {
					try {
						Thread.sleep(0, 50000);
					} catch (InterruptedException e) {
					}

				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}
			
			int userId = r.nextInt(recordCount);
			int amount = r.nextInt(maxamount);
			String txnId = "Kafka_" + tranCount + "_" + currentMs;
			String request = SINGLE_QUOTE + userId + QUOTE_COMMA_QUOTE + amount + QUOTE_COMMA_QUOTE +txnId +  SINGLE_QUOTE ;
				
			ProducerRecord<String, String> newrec = new ProducerRecord<String, String>("ADDCREDIT",
					request);
			
			producer.send(newrec);
			
			if (tranCount++ % 10000 == 0) {
				msg("On transaction# " + tranCount + ", user,amount,txnid= " + request);
			}

		}
		
		producer.flush();
		producer.close();
		
	}

	/**
	 * Print a formatted message.
	 * 
	 * @param message
	 */
	public static void msg(String message) {

		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = new Date();
		String strDate = sdfDate.format(now);
		System.out.println(strDate + ":" + message);

	}

}
