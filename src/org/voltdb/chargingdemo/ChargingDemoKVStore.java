package org.voltdb.chargingdemo;



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

import java.util.Arrays;
import org.voltdb.client.Client;

public class ChargingDemoKVStore extends BaseChargingDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		msg("Parameters:" + Arrays.toString(args));

		if (args.length != 7) {
			msg("Usage: hostnames recordcount tpms durationseconds queryseconds jsonsize deltaProportion");
			System.exit(1);
		}

		// Comma delimited list of hosts...
		String hostlist = args[0];

		// How many users
		int userCount = Integer.parseInt(args[1]);


		// Target transactions per millisecond.
		int tpMs = Integer.parseInt(args[2]);

		// Runtime for TRANSACTIONS in seconds.
		int durationSeconds = Integer.parseInt(args[3]);

		// How often we do global queries...
		int globalQueryFreqSeconds = Integer.parseInt(args[4]);

		// How often we do global queries...
		int jsonsize = Integer.parseInt(args[5]);
		
		int deltaProportion = Integer.parseInt(args[6]);

	
		try {
			// A VoltDB Client object maintains multiple connections to all the
			// servers in the cluster.
			Client mainClient = connectVoltDB(hostlist);

			unlockAllRecords(mainClient);
			runKVBenchmark(userCount, tpMs, durationSeconds, globalQueryFreqSeconds, jsonsize,
					mainClient,deltaProportion);

			msg("Closing connection...");
			mainClient.close();

		} catch (Exception e) {
			msg(e.getMessage());
		}

	}


}
