package org.voltdb.chargingdemo.calbacks;

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
	public void clientCallback(ClientResponse arg0) throws Exception {

		if (arg0.getStatus() == ClientResponse.SUCCESS) {

			if (arg0.getAppStatus() == ReferenceData.STATUS_ALL_UNITS_ALLOCATED
					|| arg0.getAppStatus() == ReferenceData.STATUS_SOME_UNITS_ALLOCATED
					|| arg0.getAppStatus() == ReferenceData.STATUS_NO_MONEY) {

				userTransactionState.endTran();

				VoltTable balanceTable = arg0.getResults()[arg0.getResults().length - 2];
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

				} else {
					msg("ReportUsageCreditCallback user=" + userTransactionState.id + ":" + arg0.getAppStatusString());
				}

			} 
		} else {
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
