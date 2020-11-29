package org.voltdb.chargingdemo.calbacks;

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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.chargingdemo.UserTransactionState;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import chargingdemoprocs.ReferenceData;

public class ReportQuotaUsageCallback implements ProcedureCallback {

	UserTransactionState userTransactionState;

	public ReportQuotaUsageCallback(UserTransactionState userTransactionState) {
		this.userTransactionState = userTransactionState;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.voltdb.chargingdemo.ReportLatencyCallback#clientCallback(org.voltdb.
	 * client.ClientResponse)
	 */
	@Override
	public void clientCallback(ClientResponse arg0) throws Exception {

		// if the call worked....
		if (arg0.getStatus() == ClientResponse.SUCCESS) {

			// if we have an expected response...
			if (arg0.getAppStatus() == ReferenceData.STATUS_ALL_UNITS_ALLOCATED
					|| arg0.getAppStatus() == ReferenceData.STATUS_SOME_UNITS_ALLOCATED
					|| arg0.getAppStatus() == ReferenceData.STATUS_NO_MONEY
					|| arg0.getAppStatus() == ReferenceData.STATUS_OK) {

				// Report latency for users whose id is divisible by 500000...
				if (userTransactionState.id % 500000 == 0) {
					msg("ReportUsageCreditCallback user=" + userTransactionState.id + " transaction took "
							+ (System.currentTimeMillis() - userTransactionState.txStartMs) + "ms");
				}

				// Mark transaction as finished so we can start another one
				userTransactionState.endTran();

				// Get balance for user, based on finished transactions.
				VoltTable balanceTable = arg0.getResults()[arg0.getResults().length - 2];
				
			    // Get total value of outstanding reservations  
				VoltTable reservationTable = arg0.getResults()[arg0.getResults().length - 1];

				if (balanceTable.advanceRow()) {

					long balance = balanceTable.getLong("balance");
					userTransactionState.sessionId = balanceTable.getLong("sessionid");

					long reserved = 0;

					if (reservationTable.advanceRow()) {
						reserved = reservationTable.getLong("allocated_amount");
						if (reservationTable.wasNull()) {
							reserved = 0;
						}
					}

					userTransactionState.currentlyReserved = reserved;
					userTransactionState.spendableBalance = balance - reserved;

					// We should never see a negative balance...
					if (userTransactionState.spendableBalance < 0) {
						msg("ReportUsageCreditCallback user=" + userTransactionState.id + ": negative balance of "
								+ userTransactionState.spendableBalance + " seen");
					}

				} else {
					// We should never detect a nonexistent balance...
					msg("ReportUsageCreditCallback user=" + userTransactionState.id + ": doesn't have a balance");
				}

			} else {
				// We got an app status code we weren't expecting... should never happen..
				msg("ReportUsageCreditCallback user=" + userTransactionState.id + ":" + arg0.getAppStatusString());
			}
		} else {
			// We got some form of Volt error code.
			msg("ReportUsageCreditCallback user=" + userTransactionState.id + ":" + arg0.getStatusString());
		}
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
