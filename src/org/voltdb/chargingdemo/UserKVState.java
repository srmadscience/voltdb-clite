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

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import chargingdemoprocs.ReferenceData;

/**
 * Class to keep track of a user's state. It implements ProcedureCallback, which
 * means the clientCallback method is called when callProcedure finishes.
 *
 */
public class UserKVState implements ProcedureCallback {

	static final byte STATUS_UNLOCKED = 0;
	static final byte STATUS_TRYING_TO_LOCK = 1;
	static final byte STATUS_LOCKED = 2;
	static final byte STATUS_UPDATING = 3;

	/**
	 * Unique ID given to us by VoltDB that we use to prove that we are the owner of
	 * this lock.
	 */
	String lockId = null;

	/**
	 * ID of user.
	 */
	int id = 0;

	/**
	 * Where we are in the update cycle..
	 */
	int userState = STATUS_UNLOCKED;

	/**
	 * When a transaction started, or zero if there isn't one.
	 */
	long txStartMs = 0;

	/**
	 * Create a record for a user.
	 * 
	 * @param id
	 */
	public UserKVState(int id) {
		this.id = id;
		userState = STATUS_UNLOCKED;

	}

	public void setStatus(int newStatus) {
		userState = newStatus;
	}

	/**
	 * Report start of transaction.
	 */
	public void startTran() {

		txStartMs = System.currentTimeMillis();
	}

	/**
	 * @return the txInFlight
	 */
	public boolean isTxInFlight() {

		if (txStartMs > 0) {
			return true;
		}

		return false;
	}

	public int getUserStatus() {
		return userState;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String desc = "UserState [id=" + id + ", userStatus=" + userState + ", lockId=" + lockId
				+ ", productSessionIds=" + ", txStartMs=" + txStartMs + " ]";

		return desc;
	}

	@Override
	public void clientCallback(ClientResponse arg0) throws Exception {

		if (arg0.getStatus() == ClientResponse.SUCCESS) {

			byte statusByte = arg0.getAppStatus();

			if (userState == STATUS_UNLOCKED) {

			} else if (userState == STATUS_TRYING_TO_LOCK) {

				if (statusByte == ReferenceData.STATUS_RECORD_HAS_BEEN_SOFTLOCKED
						|| statusByte == ReferenceData.STATUS_RECORD_ALREADY_SOFTLOCKED) {

					userState = STATUS_LOCKED;
					lockId = arg0.getAppStatusString();

				} else {
					userState = STATUS_UNLOCKED;
				}
			} else if (userState == STATUS_UPDATING) {

				lockId = "";
				userState = STATUS_UNLOCKED;
			}

		}

		txStartMs = 0;
	}

	/**
	 * @return the lockId
	 */
	public String getLockId() {
		return lockId;
	}

	/**
	 * @param lockId the lockId to set
	 */
	public void setLockId(String lockId) {
		this.lockId = lockId;
	}

}
