package org.voltdb.chargingdemo;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

public class ChargingDemoUntil extends BaseChargingDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		msg("Parameters:" + Arrays.toString(args));

		if (args.length != 9) {
			msg("Usage: hostnames recordcount offset tpms durationseconds queryseconds pausems inctpms endlatency");
			System.exit(1);
		}

		// Comma delimited list of hosts...
		String hostlist = args[0];

		// How many users
		int userCount = Integer.parseInt(args[1]);

		// Used to allow multiple copies of client to run at once. Makes demo start
		// creating ids
		// from 'offset' instead of zero.
		int offset = Integer.parseInt(args[2]);

		// Target transactions per millisecond.
		int tpMs = Integer.parseInt(args[3]);

		// Runtime for TRANSACTIONS in seconds.
		int durationSeconds = Integer.parseInt(args[4]);

		// How often we do global queries...
		int globalQueryFreqSeconds = Integer.parseInt(args[5]);

		// How often we do global queries...
		int pauseMs = Integer.parseInt(args[6]);

		// How often we do global queries...
		int inctpms = Integer.parseInt(args[7]);

		// How often we do global queries...
		int endlatency = Integer.parseInt(args[8]);

		// In some cases we might want to run a check at the
		// end of the benchmark that all of our transactions did in fact happen.
		// the 'state' array contains a model of what things *ought* to look like.
		UserTransactionState[] state = new UserTransactionState[userCount];

		try {
			// A VoltDB Client object maintains multiple connections to all the
			// servers in the cluster.
			Client mainClient = connectVoltDB(hostlist);

			long maxSeenLatency = 0;
			int tpMsThisPass = tpMs;

			clearUnfinishedTransactions(mainClient);

			while (true) {

				maxSeenLatency = runTransactionBenchmark(userCount, offset, tpMs, durationSeconds, globalQueryFreqSeconds, state,
						mainClient);

				tpMsThisPass = tpMsThisPass + inctpms;

				msg("tps now " + tpMsThisPass + ", latency was " + maxSeenLatency);

				if (maxSeenLatency > endlatency) {
					msg("stopping with latency of " + maxSeenLatency);
					break;
				}

				msg("waiting for " + pauseMs + " ms");
				Thread.sleep(pauseMs);

			}

			msg("Closing connection...");
			mainClient.close();

		} catch (Exception e) {
			msg(e.getMessage());
		}

	}

}
