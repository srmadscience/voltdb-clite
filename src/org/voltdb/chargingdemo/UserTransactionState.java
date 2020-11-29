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

/**
 * Class to keep track of how many transactions a given user has. It also keeps
 * track of whether a transaction is in progress and when it started.
 * 
 * @author drolfe
 *
 */
public class UserTransactionState {

  
  /**
   * ID of user.
   */
  public int id = 0;

 
  public long sessionId = -1;
  
  /**
   * When a transaction started, or zero if there isn't one.
   */
  public long txStartMs = 0;

  /**
   * Balance,
   */
  public long spendableBalance = 0;
  
  public long currentlyReserved = 0;

  /**
   * Create a record for a user.
   * 
   * @param id
   */
  public UserTransactionState(int id,long spendableBalance) {
    this.id = id;
    this.spendableBalance =  spendableBalance;
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

  /**
   * We measure latency by comparing when this call happens to when startTran
   * was called.
   * 
   * @param productId
   * @param sessionid
   * @param statusByte
   */
  public void reportEndTransaction(long sessionid, byte statusByte, long spendableBalance) {

 
    txStartMs = 0;
    this.sessionId = sessionid;
    this.spendableBalance = spendableBalance;

  }


public void endTran() {
	txStartMs = 0;
	
}




  
}
