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
import java.util.Random;

import org.voltdb.client.Client;

import com.google.gson.Gson;

public class CreateChargingDemoData extends BaseChargingDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Gson gson = new Gson();
		Random r = new Random();

		msg("Parameters:" + Arrays.toString(args));

		if (args.length != 4 ) {
			msg("Usage: hostnames recordcount tpms  initialcredit ");
			System.exit(1);
		}

		// Comma delimited list of hosts...
		String hostlist = args[0];

		// How many users
		int userCount = Integer.parseInt(args[1]);

		// Target transactions per millisecond.
		int tpMs = Integer.parseInt(args[2]);

		// How long our arbitrary JSON payload will be.
		int loblength = 120;
		final String ourJson = getExtraUserDataAsJsonString(loblength, gson, r);

		// Default credit users are 'born' with
		int initialCredit = Integer.parseInt(args[3]);
	
		try {
			// A VoltDB Client object maintains multiple connections to all the
			// servers in the cluster.
			Client mainClient = connectVoltDB(hostlist);
			
			upsertAllUsers(userCount, tpMs, ourJson, initialCredit, mainClient);

			msg("Closing connection...");
			mainClient.close();

		} catch (Exception e) {
			msg(e.getMessage());
		}

	}






	

}
