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
import org.voltdb.VoltType;

public class ReportQuotaUsage extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt(
			"SELECT userid FROM user_table WHERE userid = ?;");

	public static final SQLStmt getTxn = new SQLStmt("SELECT txn_time FROM user_recent_transactions "
			+ "WHERE userid = ? AND user_txn_id = ?;");

	public static final SQLStmt getUserBalance = new SQLStmt("SELECT balance, CAST(? AS BIGINT) sessionid FROM user_balance WHERE userid = ?;");

	public static final SQLStmt getCurrrentlyAllocated = new SQLStmt(
			"select nvl(sum(allocated_amount),0)  allocated_amount from user_usage_table where userid = ?;");

	public static final SQLStmt addTxn = new SQLStmt("INSERT INTO user_recent_transactions "
			+ "(userid, user_txn_id, txn_time, approved_amount,spent_amount,purpose,sessionid) VALUES (?,?,NOW,?,?,?,?);");

	public static final SQLStmt delOldUsage = new SQLStmt(
			"DELETE FROM user_usage_table WHERE userid = ? AND sessionid = ?;");

	public static final SQLStmt delOldUsageHouseKeeping = new SQLStmt(
			"DELETE FROM user_usage_table WHERE userid = ? AND lastdate < DATEADD(MINUTE, -5, NOW);");

	public static final SQLStmt reportFinancialEvent = new SQLStmt(
			"INSERT INTO user_financial_events " + "(userid,amount,user_txn_id,message) VALUES (?,?,?,?);");

	public static final SQLStmt createAllocation = new SQLStmt("INSERT INTO user_usage_table "
			+ "(userid, allocated_amount,sessionid, lastdate) VALUES (?,?,?,NOW);");

	// @formatter:on

	public VoltTable[] run(long userId, int unitsUsed, int unitsWanted, long inputSessionId, String txnId)
			throws VoltAbortException {

		long sessionId = inputSessionId;

		if (sessionId <= 0) {
			sessionId = this.getUniqueId();
		}

		voltQueueSQL(getUser, userId);
		voltQueueSQL(getTxn, userId, txnId);

		VoltTable[] results1 = voltExecuteSQL();

		// Sanity check: Does this user exist?
		if (!results1[0].advanceRow()) {
			throw new VoltAbortException("User " + userId + " does not exist");
		}

		// Sanity Check: Is this a re-send of a transaction we've already done?
		if (results1[1].advanceRow()) {
			this.setAppStatusCode(ReferenceData.TXN_ALREADY_HAPPENED);
			this.setAppStatusString(
					"Event already happened at " + results1[1].getTimestampAsTimestamp("txn_time").toString());
			return voltExecuteSQL(true);
		}

		long amountSpent = unitsUsed * -1;
		String decision = "Spent " + amountSpent;

		// Update balance
		voltQueueSQL(reportFinancialEvent, userId, amountSpent, txnId, "Spent " + amountSpent);

		// Delete old usage record
		voltQueueSQL(delOldUsage, userId, sessionId);
		voltQueueSQL(delOldUsageHouseKeeping, userId);
		voltQueueSQL(getUserBalance, sessionId, userId);
		voltQueueSQL(getCurrrentlyAllocated, userId);

		if (unitsWanted == 0) {
			voltQueueSQL(addTxn, userId, txnId, 0,amountSpent, decision,sessionId);
			voltQueueSQL(getUserBalance, sessionId, userId);
			voltQueueSQL(getCurrrentlyAllocated, userId);

			this.setAppStatusCode(ReferenceData.STATUS_OK);
			return voltExecuteSQL(true);
		}

		VoltTable[] results2 = voltExecuteSQL();

		VoltTable userBalance = results2[3];
		VoltTable allocated = results2[4];

		// Calculate how much money is actually available...

		userBalance.advanceRow();
		long availableCredit = userBalance.getLong("balance");

		if (allocated.advanceRow()) {
			availableCredit = availableCredit - allocated.getLong("allocated_amount");
		}

		long amountApproved = 0;
		
		if (availableCredit < 0) {

			decision = decision + "; Negative balance: " + availableCredit;
			this.setAppStatusCode(ReferenceData.STATUS_NO_MONEY);

		} else if (unitsWanted > availableCredit) {

			amountApproved  = availableCredit;
			decision = decision + "; Allocated " + availableCredit + " units of " + unitsWanted + " asked for";
			this.setAppStatusCode(ReferenceData.STATUS_SOME_UNITS_ALLOCATED);

		} else {
			
			amountApproved  = unitsWanted;
			decision = decision + "; Allocated " + unitsWanted;
			this.setAppStatusCode(ReferenceData.STATUS_ALL_UNITS_ALLOCATED);
	
		}

		voltQueueSQL(createAllocation, userId, amountApproved, sessionId);
		
     	this.setAppStatusString(decision);
		// Note that transaction is now 'official'
		
		voltQueueSQL(addTxn, userId, txnId, amountApproved, amountSpent, decision,sessionId);
	
		voltQueueSQL(getUserBalance, sessionId, userId);
		voltQueueSQL(getCurrrentlyAllocated, userId);

		return voltExecuteSQL();

	}

}
