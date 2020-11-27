package org.voltdb.chargingdemo.calbacks;

import org.voltdb.VoltTable;
import org.voltdb.chargingdemo.ChargingDemoTransactions;
import org.voltdb.chargingdemo.UserTransactionState;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

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

public class UpdateSessionStateCallback implements ProcedureCallback {

  UserTransactionState[] state = null;
  int offset = 0;

  public UpdateSessionStateCallback(UserTransactionState[] state, int offset) {
    super();
    this.state = state;
    this.offset = offset;
  }

  public void clientCallback(ClientResponse arg0) throws Exception {
    if (arg0.getStatus() != ClientResponse.SUCCESS) {
      ChargingDemoTransactions.msg("Error Code " + arg0.getStatusString());
    } else {

      // Find id. It'll be in the  last VoltTable..
      VoltTable balanceTable = arg0.getResults()[arg0.getResults().length - 1];

      if (balanceTable.advanceRow()) {
        
        int userid = (int) balanceTable.getLong("userid");
        long balance = balanceTable.getLong("availableCredit");
        long sessionid = balanceTable.getLong("sessionid");
        int productid = (int) balanceTable.getLong("productid");  
        long allocated  = balanceTable.getLong("allocated_units"); 
  
        synchronized (state) {
          state[userid - offset].reportBalance(balance);
          state[userid - offset].reportEndTransaction(productid, sessionid, arg0.getStatus(),allocated);
        }


      } else {
        
          ChargingDemoTransactions.msg("Balance info not found " + balanceTable.toFormattedString());
        
      }

    }

  }

}
