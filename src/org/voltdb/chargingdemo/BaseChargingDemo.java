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
import java.util.Date;
import java.util.Random;

import org.voltdb.chargingdemo.calbacks.AddCreditCallback;
import org.voltdb.chargingdemo.calbacks.ComplainOnErrorCallback;

import org.voltdb.chargingdemo.calbacks.ReportQuotaUsageCallback;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import com.google.gson.Gson;

import chargingdemoprocs.ExtraUserData;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 */
public abstract class BaseChargingDemo {

	public static final long GENERIC_QUERY_USER_ID = 42;

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

	/**
	 * 
	 * Delete all users in a range at tpMs per second
	 * 
	 * @param minId
	 * @param maxId
	 * @param tpMs
	 * @param mainClient
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws NoConnectionsException
	 */
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
			ComplainOnErrorCallback deleteUserCallback = new ComplainOnErrorCallback();
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

	/**
	 * Create userCount users at tpMs per second.
	 * 
	 * @param userCount
	 * @param tpMs
	 * @param ourJson
	 * @param initialCredit
	 * @param mainClient
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws NoConnectionsException
	 */
	protected static void upsertAllUsers(int userCount, int tpMs, String ourJson, int initialCredit, Client mainClient)
			throws InterruptedException, IOException, NoConnectionsException {
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

			ComplainOnErrorCallback upsertUserCallback = new ComplainOnErrorCallback();

			mainClient.callProcedure(upsertUserCallback, "UpsertUser", i, initialCredit, ourJson, "Created",
					new Date(startMsUpsert), "Create_" + i);

			if (i % 100000 == 1) {
				msg("Upserted " + i + " users...");

			}

		}

		msg("All " + userCount + " entries in queue, waiting for it to drain...");
		mainClient.drain();

		long entriesPerMS = userCount / (System.currentTimeMillis() - startMsUpsert);
		msg("Upserted " + entriesPerMS + " users per ms...");
	}

	/**
	 * Convenience method to query a user a general stats and log the results.
	 * 
	 * @param mainClient
	 * @param queryUserId
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static void queryUserAndStats(Client mainClient, long queryUserId)
			throws IOException, NoConnectionsException, ProcCallException {

		// Query user #queryUserId...
		msg("Query user #" + queryUserId + "...");
		ClientResponse userResponse = mainClient.callProcedure("GetUser", queryUserId);

		for (int i = 0; i < userResponse.getResults().length; i++) {
			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
		}

		msg("Show amount of credit currently reserved for products...");
		ClientResponse allocResponse = mainClient.callProcedure("ShowCurrentAllocations__promBL");

		for (int i = 0; i < allocResponse.getResults().length; i++) {
			msg(System.lineSeparator() + allocResponse.getResults()[i].toFormattedString());
		}
	}

	/**
	 * 
	 * Convenience method to query all users who have a specific loyalty card id
	 * 
	 * @param mainClient
	 * @param cardId
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static void queryLoyaltyCard(Client mainClient, long cardId)
			throws IOException, NoConnectionsException, ProcCallException {

		// Query user #queryUserId...
		msg("Query card #" + cardId + "...");
		ClientResponse userResponse = mainClient.callProcedure("FindByLoyaltyCard", cardId);

		for (int i = 0; i < userResponse.getResults().length; i++) {
			msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
		}

	}

	/**
	 * Convenience method to remove unneeded records storing old allotments of
	 * credit.
	 * 
	 * @param mainClient
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static void clearUnfinishedTransactions(Client mainClient)
			throws IOException, NoConnectionsException, ProcCallException {

		msg("Clearing unfinished transactions from prior runs...");

		mainClient.callProcedure("@AdHoc", "DELETE FROM user_usage_table;");
		msg("...done");

	}

	/**
	 * 
	 * Convenience method to clear outstaning locks between runs
	 * 
	 * @param mainClient
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static void unlockAllRecords(Client mainClient)
			throws IOException, NoConnectionsException, ProcCallException {

		msg("Clearing locked sessions from prior runs...");

		mainClient.callProcedure("@AdHoc",
				"UPDATE user_table SET user_softlock_sessionid = null, user_softlock_expiry = null WHERE user_softlock_sessionid IS NOT NULL;");
		msg("...done");

	}

	/**
	 * 
	 * Run a transaction benchmark for userCount users at tpMs per ms.
	 * 
	 * @param userCount              number of users
	 * @param tpMs                   transactions per milliseconds
	 * @param durationSeconds
	 * @param globalQueryFreqSeconds how often we check on global stats and a single
	 *                               user
	 * @param mainClient
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static long runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
			int globalQueryFreqSeconds, Client mainClient)
			throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

		Random r = new Random();

		UserTransactionState[] users = new UserTransactionState[userCount];

		msg("Creating client records for " + users.length + " users");
		for (int i = 0; i < users.length; i++) {
			users[i] = new UserTransactionState(i, -1);
		}

		final long startMsRun = System.currentTimeMillis();
		long currentMs = System.currentTimeMillis();
		int tpThisMs = 0;

		final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000);

		// How many transactions we've done...
		long tranCount = 0;
		long inFlightCount = 0;
		long addCreditCount = 0;
		long reportUsageCount = 0;

		msg("starting...");

		while (endtimeMs > System.currentTimeMillis()) {

			if (tpThisMs++ > tpMs) {

				while (currentMs == System.currentTimeMillis()) {
					Thread.sleep(0, 50000);

				}

				currentMs = System.currentTimeMillis();
				tpThisMs = 0;
			}

			int randomuser = r.nextInt(userCount);

			if (users[randomuser].isTxInFlight()) {
				inFlightCount++;
				tpThisMs--;
			} else {

				users[randomuser].startTran();

				if (users[randomuser].spendableBalance < 1000) {

					addCreditCount++;

					final long extraCredit = r.nextInt(1000) + 1000;

					AddCreditCallback addCreditCallback = new AddCreditCallback(users[randomuser]);

					mainClient.callProcedure(addCreditCallback, "AddCredit", randomuser, extraCredit,
							"AddCreditOnShortage" + "_" + System.currentTimeMillis());

				} else {

					reportUsageCount++;

					ReportQuotaUsageCallback reportUsageCallback = new ReportQuotaUsageCallback(users[randomuser]);

					long unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
					long unitsWanted = r.nextInt(100);

					mainClient.callProcedure(reportUsageCallback, "ReportQuotaUsage", randomuser, unitsUsed,
							unitsWanted, users[randomuser].sessionId,
							"ReportQuotaUsage" + "_" + System.currentTimeMillis());

				}
			}

			if (tranCount++ % 100000 == 0) {
				msg("On transaction #" + tranCount);
			}
			;
		}

		msg("finished adding transactions to queue");
		mainClient.drain();
		msg("Queue drained");

		long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
		msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

		long tps = (tranCount / elapsedTimeMs) / 1000;

		msg("TPS = " + tps);

		msg("Add Credit calls = " + addCreditCount);
		msg("Report Usage calls = " + reportUsageCount);
		msg("Skipped because transaction was in flight = " + inFlightCount);

		return tps;
	}

	/**
	 * 
	 * Run a key value store benchmark for userCount users at tpMs transactions per
	 * millisecond and with deltaProportion records sending the entire record.
	 * 
	 * @param userCount
	 * @param tpMs
	 * @param durationSeconds
	 * @param globalQueryFreqSeconds
	 * @param jsonsize
	 * @param mainClient
	 * @param deltaProportion
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	protected static long runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
			int jsonsize, Client mainClient, int deltaProportion)
			throws InterruptedException, IOException, NoConnectionsException, ProcCallException {

		long lastGlobalQueryMs = 0;

		UserKVState[] userState = new UserKVState[userCount];

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

				if (deltaProportion > r.nextInt(101)) {
					mainClient.callProcedure(userState[oursession], "UpdateLockedUser", oursession,
							userState[oursession].lockId, getNewLoyaltyCardNumber(r), ExtraUserData.NEW_LOYALTY_NUMBER);
				} else {
					mainClient.callProcedure(userState[oursession], "UpdateLockedUser", oursession,
							userState[oursession].lockId, getExtraUserDataAsJsonString(jsonsize, gson, r), null);
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

			queryUserAndStats(mainClient, GENERIC_QUERY_USER_ID);

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

		msg("Stats Histogram:");

		return tranCount;
	}

	/**
	 * Return a loyalty card number
	 * 
	 * @param r
	 * @return a random loyalty card number between 0 and 1 million
	 */
	private static long getNewLoyaltyCardNumber(Random r) {
		return System.currentTimeMillis() % 1000000;
	}

}
