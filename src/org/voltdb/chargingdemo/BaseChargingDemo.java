package org.voltdb.chargingdemo;

import java.io.IOException;

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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.chargingdemo.calbacks.AddCreditCallback;
import org.voltdb.chargingdemo.calbacks.ReportLatencyCallback;
import org.voltdb.chargingdemo.calbacks.UpdateSessionStateCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;
import org.voltdb.voltutil.stats.StatsHistogram;

import com.google.gson.Gson;

import chargingdemoprocs.ExtraUserData;

public class BaseChargingDemo {

	// Possible values for 'TASK'
	public static final String TASK_TRANSACTIONS = "TRANSACTIONS";
	public static final String TASK_USERS = "USERS";
	public static final String TASK_RUN = "RUN";
	public static final String TASK_DELETE = "DELETE";

	public static final String[] PRODUCT_NAMES = { "Our Web Site", "SMS messages", "Domestic Internet Access per GB",
			"Roaming Internet Access per GB", "Domestic calls per minute" };

	public static final int[] PRODUCT_PRICES = { 0, 1, 20, 342, 3 };

	public static final String[] CLUSTER_NAMES = { "notxdcr", "jersey", "badger", "rosal" }; // TODO

	public static final int[] CLUSTER_IDS = { 0, 4, 5, 6 }; // TODO

	public static final int[] WATCHED_BY_CLUSTER_IDS = { 0, 5, 6, 4 }; // TODO

	protected static final long GENERIC_QUERY_USER_ID = 42;

	protected static long chooseTopUpAmount(long balance, Random r) {
		if (balance > 0) {
			return 100 + r.nextInt(3000);
		}

		return 100 + r.nextInt(3000) + (-1 * balance);

	}

	/**
	 * @param shc
	 * @param oneLineSummary
	 */
	protected static void getProcPercentiles(SafeHistogramCache shc, StringBuffer oneLineSummary, String procName) {

		StatsHistogram rqu = shc.get(procName);
		oneLineSummary.append((int) rqu.getLatencyAverage());
		oneLineSummary.append(':');

		oneLineSummary.append(rqu.getLatencyPct(50));
		oneLineSummary.append(':');

		oneLineSummary.append(rqu.getLatencyPct(99));
		oneLineSummary.append(':');
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

	/**
	 * Connect to VoltDB using a comma delimited hostname list.
	 * 
	 * @param commaDelimitedHostnames
	 * @return
	 * @throws Exception
	 */
	protected static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
		Client client = null;
		ClientConfig config = null;

		try {
			msg("Logging into VoltDB");

			config = new ClientConfig(); // "admin", "idontknow");
			config.setTopologyChangeAware(true);
			config.setReconnectOnConnectionLoss(true);

			client = ClientFactory.createClient(config);

			String[] hostnameArray = commaDelimitedHostnames.split(",");

			for (int i = 0; i < hostnameArray.length; i++) {
				msg("Connect to " + hostnameArray[i] + "...");
				try {
					client.createConnection(hostnameArray[i]);
				} catch (Exception e) {
					msg(e.getMessage());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
		}

		return client;

	}

	/**
	 * Convenience method to generate a JSON payload.
	 * 
	 * @param length
	 * @return
	 */
	protected static String getExtraUserDataAsJsonString(int length, Gson gson, Random r) {

		ExtraUserData eud = new ExtraUserData();

		eud.loyaltySchemeName = "HelperCard";
		eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r);

		StringBuffer ourText = new StringBuffer();

		for (int i = 0; i < length / 2; i++) {
			ourText.append(Integer.toHexString(r.nextInt(256)));
		}

		eud.mysteriousHexPayload = ourText.toString();

		return gson.toJson(eud);
	}

	protected static void confirmMetadataExists(Client mainClient)
			throws IOException, NoConnectionsException, ProcCallException {
		// Make sure required metadata exists...
		for (int i = 0; i < PRODUCT_NAMES.length; i++) {
			ClientResponse cr = mainClient.callProcedure("@AdHoc",
					"SELECT * FROM product_table where productid = " + i + ";");

			if (!cr.getResults()[0].advanceRow()) {
				mainClient.callProcedure("product_table.insert", i, PRODUCT_NAMES[i], PRODUCT_PRICES[i]);
			}
		}

		for (int i = 0; i < CLUSTER_NAMES.length; i++) {
			ClientResponse cr = mainClient.callProcedure("@AdHoc",
					"SELECT * FROM cluster_table where cluster_id = " + CLUSTER_IDS[i] + ";");

			if (!cr.getResults()[0].advanceRow()) {
				mainClient.callProcedure("cluster_table.insert", CLUSTER_IDS[i], CLUSTER_NAMES[i],
						WATCHED_BY_CLUSTER_IDS[i], 4);
			}
		}
	}

	protected static void deleteAllUsers(int minId, int maxId, int tpMs, Client mainClient)
			throws InterruptedException, IOException, NoConnectionsException {

		msg("Deleting users from " + minId + " to " + maxId);

		final long startMsDelete = System.currentTimeMillis();
		long currentMs = System.currentTimeMillis();

		// To make sure we do things at a consistent rate (tpMs) we
		// track how many transactions we've queued this ms and sleep if
		// we've reached our limit.
		int tpThisMs = 0;

		// So we iterate through all our users...
		for (int i = minId; i <= maxId; i++) {

			if (tpThisMs++ > tpMs) {

				// but sleep if we're moving too fast...
				while (currentMs == System.currentTimeMillis()) {
					Thread.sleep(0, 50000);
				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}

			// Put a request to delete a user into the queue.
			ReportLatencyCallback deleteUserCallback = new ReportLatencyCallback("DelUser");
			mainClient.callProcedure(deleteUserCallback, "DelUser", i);

			if (i % 100000 == 1) {
				msg("Deleted " + i + " users...");
			}

		}

		// Because we've put messages into the clients queue we
		// need to wait for them to be processed.
		msg("All " + (maxId - minId + 1) + " entries in queue, waiting for it to drain...");
		mainClient.drain();

		final long entriesPerMs = (maxId - minId + 1) / (System.currentTimeMillis() - startMsDelete);
		msg("Deleted " + entriesPerMs + " users per ms...");
	}

	protected static void upsertAllUsers(int userCount, int offset, int tpMs, final String ourJson, int initialCredit,
			Client mainClient) throws InterruptedException, IOException, NoConnectionsException {
		final long startMsUpsert = System.currentTimeMillis();
		long currentMs = System.currentTimeMillis();
		int tpThisMs = 0;

		for (int i = 0; i < userCount; i++) {

			if (tpThisMs++ > tpMs) {

				while (currentMs == System.currentTimeMillis()) {
					Thread.sleep(0, 50000);
				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}

			ReportLatencyCallback upsertUserCallback = new ReportLatencyCallback("UpsertUser");

			mainClient.callProcedure(upsertUserCallback, "UpsertUser", i + offset, initialCredit,  ourJson,
					"Created", new Date(startMsUpsert), "Create_" + i);

			if (i % 100000 == 1) {
				msg("Upserted " + i + " users...");

			}

		}

		msg("All " + userCount + " entries in queue, waiting for it to drain...");
		mainClient.drain();

		long entriesPerMs = userCount / (System.currentTimeMillis() - startMsUpsert);
		msg("Upserted " + entriesPerMs + " users per ms...");
	}

	protected static void queryUser(Client mainClient, long queryUserId)
			throws IOException, NoConnectionsException, ProcCallException {

		SafeHistogramCache shc = SafeHistogramCache.getInstance();

		// Query user #queryUserId...
		msg("Query user #" + queryUserId + "...");
		final long startQueryUserMs = System.currentTimeMillis();
		ClientResponse userResponse = mainClient.callProcedure("GetUser", queryUserId);
		shc.reportLatency("GetUser", startQueryUserMs, "", 50);

		for (int i = 0; i < userResponse.getResults().length; i++) {
			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
		}

		msg("Show amount of credit currently reserved for products...");
		final long startQueryAllocationsMs = System.currentTimeMillis();
		ClientResponse allocResponse = mainClient.callProcedure("ShowCurrentAllocations__promBL");
		shc.reportLatency("ShowCurrentAllocations__promBL", startQueryAllocationsMs, "", 50);

		for (int i = 0; i < allocResponse.getResults().length; i++) {
			msg(System.lineSeparator() + allocResponse.getResults()[i].toFormattedString());
		}
	}
	protected static void queryLoyaltyCard(Client mainClient, long cardId)
			throws IOException, NoConnectionsException, ProcCallException {

		SafeHistogramCache shc = SafeHistogramCache.getInstance();

		// Query user #queryUserId...
		msg("Query card #" + cardId + "...");
		final long startQueryUserMs = System.currentTimeMillis();
		ClientResponse userResponse = mainClient.callProcedure("FindByLoyaltyCard", cardId);
		shc.reportLatency("FindByLoyaltyCard", startQueryUserMs, "", 50);

		for (int i = 0; i < userResponse.getResults().length; i++) {
			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
		}

	}

	protected static void clearUnfinishedTransactions(Client mainClient)
			throws IOException, NoConnectionsException, ProcCallException {

		msg("Clearing unfinished transactions from prior runs...");

		//TODO make XDCR friendly
		mainClient.callProcedure("@AdHoc", "DELETE FROM user_usage_table;");
		msg("...done");

	}
	
	protected static void unlockAllRecords(Client mainClient)
			throws IOException, NoConnectionsException, ProcCallException {

		msg("Clearing locked sessions from prior runs...");

		//TODO make XDCR friendly
		mainClient.callProcedure("@AdHoc", "UPDATE user_table SET user_softlock_sessionid = null, user_softlock_expiry = null WHERE user_softlock_sessionid IS NOT NULL;");
		msg("...done");

	}

	protected static long runTransactionBenchmark(int userCount, int offset, int tpMs, int durationSeconds,
			int globalQueryFreqSeconds, UserTransactionState[] state, Client mainClient)
			throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

		long lastGlobalQueryMs = 0;

		// UpdateSessionStateCallback examines responses and updates the sessionId
		// for a
		// user. SessionId is created inside a VoltDB procedure.
		UpdateSessionStateCallback ussc = new UpdateSessionStateCallback(state, offset);

		SafeHistogramCache shc = SafeHistogramCache.getInstance();

		Random r = new Random();

		// Tell the system everyone has zero credit, even though that's probably
		// not true. This will result in lots of AddCredits, by the end of which
		// state will be up to date.
		for (int i = 0; i < userCount; i++) {

			if (state[i] == null) {
				state[i] = new UserTransactionState(i, 0);
				state[i].IncUserStatus();
			}
		}

		final long startMsRun = System.currentTimeMillis();
		long currentMs = System.currentTimeMillis();
		int tpThisMs = 0;

		final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000);

		// How many transactions we've done...
		int tranCount = 0;
		int inFlightCount = 0;

		while (endtimeMs > System.currentTimeMillis()) {

			if (tpThisMs++ > tpMs) {

				while (currentMs == System.currentTimeMillis()) {
					Thread.sleep(0, 50000);

				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}

			// Find session to do a transaction for...
			int oursession = r.nextInt(userCount);

			// See if session already has an active transaction and avoid
			// it if it does.

			if (state[oursession].isTxInFlight()) {
				inFlightCount++;
			} else {

				int ourProduct = r.nextInt(PRODUCT_NAMES.length);
				long sessionId = UserTransactionState.SESSION_NOT_STARTED;

				// Come up with reports on how much we used and how much we want...

				// usedUnits is usually less than what we requested last time.

				final int requestUnits = 50 + r.nextInt(49);
				long usedUnits = r.nextInt(50);

				// state[oursession].getUserStatus() will be zero (STATUS_NEW_USER)
				// the first time we access a session.

				sessionId = state[oursession].getProductSessionId(ourProduct);

				if (sessionId == UserTransactionState.SESSION_NOT_STARTED) {
					usedUnits = 0;
				} else if (state[oursession].getProductAllocation(ourProduct) < usedUnits) {
					usedUnits = state[oursession].getProductAllocation(ourProduct);
				}

				// Every ADD_CREDIT_INTERVAL we add credit instead of using it...
				if (state[oursession].getBalance() < 1000) {

					final long extraCredit = chooseTopUpAmount(state[oursession].getBalance(), r);

					AddCreditCallback addCreditCallback = new AddCreditCallback("AddCredit", state, oursession, offset);
					mainClient.callProcedure(addCreditCallback, "AddCredit", oursession + offset, extraCredit,
							"AddCreditOnShortage" + "_" + state[oursession].getUserStatus() + "_" + tranCount + "_"
									+ extraCredit);

				} else {
					// Otherwise report how much credit we used and ask for more...
					state[oursession].startTran();

					mainClient.callProcedure(ussc, "ReportQuotaUsage", oursession + offset, ourProduct, usedUnits,
							requestUnits, sessionId, "ReportQuotaUsage" + "_" + state[oursession].getUserStatus() + "_"
									+ tranCount + "_" + usedUnits + "_" + ourProduct);

				}

				state[oursession].IncUserStatus();

				tranCount++;
			}

			if (tranCount % 100000 == 1) {
				msg("Transaction " + tranCount);

			}

			// See if we need to do global queries...
			if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000) < System.currentTimeMillis()) {
				lastGlobalQueryMs = System.currentTimeMillis();

				queryUser(mainClient, GENERIC_QUERY_USER_ID);

			}

		}

		msg(tranCount + " transactions done...");
		msg("All entries in queue, waiting for it to drain...");
		mainClient.drain();
		msg("Queue drained...");

		long transactionsPerMs = tranCount / (System.currentTimeMillis() - startMsRun);
		msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");
		msg(inFlightCount + " events where a tx was in flight were observed");
		msg("Waiting 10 seconds - if we are using XDCR we need to wait for remote transactions to reach us");
		Thread.sleep(10000);

		msg(getSummaryStats(shc, tpMs, transactionsPerMs).toString());

		msg("Stats Histogram:");
		msg(shc.toString());

		return ((long) shc.get("ReportQuotaUsage").getLatencyAverage());
	}

	protected static long runKVBenchmark(int userCount, int offset, int tpMs, int durationSeconds,
			int globalQueryFreqSeconds, int jsonsize, Client mainClient, int updateProportion)
			throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

		long lastGlobalQueryMs = 0;

		UserKVState[] userState = new UserKVState[userCount];

		SafeHistogramCache shc = SafeHistogramCache.getInstance();

		Random r = new Random();
		
		Gson gson = new Gson();

		// Tell the system everyone has zero credit, even though that's probably
		// not true. This will result in lots of AddCredits, by the end of which
		// state will be up to date.
		for (int i = 0; i < userCount; i++) {
			userState[i] = new UserKVState(i);
		}

		final long startMsRun = System.currentTimeMillis();
		long currentMs = System.currentTimeMillis();
		int tpThisMs = 0;

		final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000);

		// How many transactions we've done...
		int tranCount = 0;
		int inFlightCount = 0;

		while (endtimeMs > System.currentTimeMillis()) {

			if (tpThisMs++ > tpMs) {

				while (currentMs == System.currentTimeMillis()) {
					Thread.sleep(0, 50000);

				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}

			// Find session to do a transaction for...
			int oursession = r.nextInt(userCount);

			// See if session already has an active transaction and avoid
			// it if it does.

			if (userState[oursession].isTxInFlight()) {

				inFlightCount++;

			} else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

				userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
				mainClient.callProcedure(userState[oursession], "GetAndLockUser", oursession);

			} else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED) {

				userState[oursession].setStatus(UserKVState.STATUS_UPDATING);
				
				if (updateProportion > r.nextInt(101)) {
					mainClient.callProcedure(userState[oursession], "UpdateLockedUser", oursession, userState[oursession].lockId, getNewLoyaltyCardNumber(r) , ExtraUserData.NEW_LOYALTY_NUMBER);
				} else {
					mainClient.callProcedure(userState[oursession], "UpdateLockedUser", oursession, userState[oursession].lockId, getExtraUserDataAsJsonString(jsonsize,gson,r), null);
				}
				
			}
			
			tranCount++;

			if (tranCount % 100000 == 1) {
				msg("Transaction " + tranCount);
			}

		}


		// See if we need to do global queries...
		if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000) < System.currentTimeMillis()) {
			lastGlobalQueryMs = System.currentTimeMillis();

			queryUser(mainClient, GENERIC_QUERY_USER_ID);

		}

		msg(tranCount + " transactions done...");
		msg("All entries in queue, waiting for it to drain...");
		mainClient.drain();
		msg("Queue drained...");

		long transactionsPerMs = tranCount / (System.currentTimeMillis() - startMsRun);
		msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");
		msg(inFlightCount + " events where a tx was in flight were observed");
		msg("Waiting 10 seconds - if we are using XDCR we need to wait for remote transactions to reach us");
		Thread.sleep(10000);

		msg(getSummaryStats(shc, tpMs, transactionsPerMs).toString());

		msg("Stats Histogram:");
		msg(shc.toString());

		return ((long) shc.get("GetAndLockUser").getLatencyAverage());
	}

	private static long getNewLoyaltyCardNumber(Random r) {
		return System.currentTimeMillis() % 10000000;
	}

	private static StringBuffer getSummaryStats(SafeHistogramCache shc, int tpMs, long transactionsPerMs) {
		StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

		oneLineSummary.append(tpMs);
		oneLineSummary.append(':');

		oneLineSummary.append(transactionsPerMs);
		oneLineSummary.append(':');

		getProcPercentiles(shc, oneLineSummary, "ReportQuotaUsage");

		getProcPercentiles(shc, oneLineSummary, "UpdateSession");

		getProcPercentiles(shc, oneLineSummary, "GetUser");

		getProcPercentiles(shc, oneLineSummary, "GetAndLockUser");

		getProcPercentiles(shc, oneLineSummary, "GetAndLockUser:OK");

		getProcPercentiles(shc, oneLineSummary, "GetAndLockUser:Fail");

		getProcPercentiles(shc, oneLineSummary, "UpdateLockedUser");

		getProcPercentiles(shc, oneLineSummary, "ShowCurrentAllocations__promBL");

		return oneLineSummary;
	}

}
