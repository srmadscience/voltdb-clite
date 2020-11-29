package chargingdemoprocs;


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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class UpdateLockedUser extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt getUser = new SQLStmt("SELECT userid ,user_json_object "
			+ ",user_last_seen,user_softlock_sessionid,user_softlock_expiry, now the_current_timestamp "
			+ "FROM user_table WHERE userid = ?;");

	public static final SQLStmt removeUserLockAndUpdateJSON = new SQLStmt(
			"UPDATE user_table SET user_softlock_sessionid = NULL, user_softlock_expiry = NULL "
					+ "   ,user_json_object = ? WHERE userid = ?;");

	Gson gson = new Gson();

	// @formatter:on

	/**
	 * Update a previously locked user. 'sessionid' is the unique id that was return
	 * by GetAndLockUser, and is required for this to work.
	 * 
	 * @param userId
	 * @return
	 * @throws VoltAbortException
	 */
	public VoltTable[] run(long userId, long sessionId, String jsonPayload, String deltaOperationName)
			throws VoltAbortException {

		voltQueueSQL(getUser, userId);

		VoltTable[] userRecord = voltExecuteSQL();

		// Sanity check: Does this user exist?
		if (!userRecord[0].advanceRow()) {
			throw new VoltAbortException("User " + userId + " does not exist");
		}

		final long lockingSessionId = userRecord[0].getLong("user_softlock_sessionid");
		final String oldJsonPayload = userRecord[0].getString("user_json_object");
		final TimestampType lockingSessionExpiryTimestamp = userRecord[0]
				.getTimestampAsTimestamp("user_softlock_expiry");

		// If there is no lock or we're the ones who locked it...
		if (lockingSessionExpiryTimestamp == null || lockingSessionId == sessionId) {

			String newJsonPayload = jsonPayload;

			if (deltaOperationName != null && deltaOperationName.equals(ExtraUserData.NEW_LOYALTY_NUMBER)) {

				try {
					ExtraUserData eud = gson.fromJson(oldJsonPayload, ExtraUserData.class);
					eud.loyaltySchemeNumber = Long.parseLong(jsonPayload);
					newJsonPayload = gson.toJson(eud);
				} catch (JsonSyntaxException e) {
					throw new VoltAbortException("Json syntax exception while working with User " + userId + ", ' "
							+ oldJsonPayload + "' and '" + jsonPayload);
				} catch (NumberFormatException e) {
					throw new VoltAbortException("Invalid loyalty card number: '" + jsonPayload);
				}

			}

			voltQueueSQL(removeUserLockAndUpdateJSON, newJsonPayload, userId);
			this.setAppStatusCode(ReferenceData.STATUS_OK);
			this.setAppStatusString("User " + userId + " updated");

		} else {

			this.setAppStatusCode(ReferenceData.STATUS_RECORD_HAS_BEEN_SOFTLOCKED);
			this.setAppStatusString("User " + userId + " currently locked by session " + lockingSessionId
					+ ". Expires at " + lockingSessionExpiryTimestamp.toString());

		}

		return voltExecuteSQL(true);

	}
}
