package chargingdemoprocs;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class UpsertUser extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt("SELECT userid FROM user_table WHERE userid = ?;");

	public static final SQLStmt getTxn = new SQLStmt("SELECT txn_time FROM user_recent_transactions "
			+ "WHERE userid = ? AND user_txn_id = ?;");

	public static final SQLStmt addTxn = new SQLStmt("INSERT INTO user_recent_transactions "
			+ "(userid, user_txn_id, txn_time, approved_amount,spent_amount,purpose) VALUES (?,?,NOW,?,?,?);");
	
	public static final SQLStmt insertUser = new SQLStmt(
			"INSERT INTO user_table (userid, user_json_object,user_last_seen) "
					+ "VALUES (?,?,?);");

	public static final SQLStmt reportAddcreditEvent = new SQLStmt(
			"INSERT INTO user_financial_events " + "(userid,amount,user_txn_id,message) VALUES (?,?,?,?);");

	// @formatter:on

	/**
	 * Upsert a user.
	 * 
	 * @param userId
	 * @param addBalance
	 * @param isNew
	 * @param json
	 * @param purpose
	 * @param lastSeen
	 * @param txnId
	 * @return
	 * @throws VoltAbortException
	 */
	public VoltTable[] run(long userId, long addBalance, String json, String purpose, TimestampType lastSeen,
			String txnId) throws VoltAbortException {

		long currentBalance = 0;

		voltQueueSQL(getUser, userId);
		voltQueueSQL(getTxn, userId, txnId);

		VoltTable[] results = voltExecuteSQL();

		if (results[1].advanceRow()) {

			this.setAppStatusCode(ReferenceData.TXN_ALREADY_HAPPENED);
			this.setAppStatusString(
					"Event already happened at " + results[1].getTimestampAsTimestamp("txn_time").toString());

		} else {

			voltQueueSQL(addTxn, userId, txnId, 0, -1 * addBalance, "Upsert user");

			if (!results[0].advanceRow()) {

				final String status = "Created user " + userId + " with opening credit of " + addBalance;
				voltQueueSQL(insertUser, userId, json, lastSeen);
				voltQueueSQL(reportAddcreditEvent, userId, addBalance, txnId, "user created");
				this.setAppStatusCode(ReferenceData.STATUS_OK);
				this.setAppStatusString(status);

			} else {

				final String status = "Updated user " + userId + " - added credit of " + addBalance + "; balance now "
						+ currentBalance;

				voltQueueSQL(reportAddcreditEvent, userId, addBalance, txnId, "user upserted");
				this.setAppStatusCode(ReferenceData.STATUS_OK);
				this.setAppStatusString(status);

			}

		}
		
		return voltExecuteSQL(true);
	}
}
