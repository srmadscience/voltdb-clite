

file -inlinebatch END_OF_BATCH

DROP procedure showTransactions;

DROP procedure FindByLoyaltyCard;

DROP PROCEDURE ShowCurrentAllocations__promBL;

DROP PROCEDURE GetUser;
   
DROP PROCEDURE GetAndLockUser;
   
DROP PROCEDURE UpdateLockedUser;
   
DROP PROCEDURE UpsertUser;
   
DROP PROCEDURE DelUser;
   
DROP PROCEDURE ReportQuotaUsage;  
   
DROP PROCEDURE AddCredit;  

DROP view user_balance; 

DROP view allocated_credit;

DROP view recent_activity;

DROP view cluster_activity_by_users;

DROP view cluster_activity;

DROP view last_cluster_activity;

DROP view cluster_users;
   
DROP table user_table;
DROP table user_usage_table;
DROP table user_recent_transactions;
DROP STREAM user_financial_events;





END_OF_BATCH
