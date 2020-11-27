
load classes ../lib/gson-2.2.2.jar;

load classes ../jars/voltdb-chargingdemo.jar;

file -inlinebatch END_OF_BATCH

CREATE table user_table
(userid bigint not null primary key
,user_json_object varchar(8000)
,user_last_seen TIMESTAMP DEFAULT NOW
,user_softlock_sessionid bigint 
,user_softlock_expiry TIMESTAMP);

PARTITION TABLE user_table ON COLUMN userid;


create index ut_del on user_table(user_last_seen);

create index ut_loyaltycard on user_table (field(user_json_object, 'loyaltySchemeNumber'));

create table user_usage_table
(userid bigint not null
,allocated_amount bigint not null
,sessionid bigint  not null
,lastdate timestamp not null
,primary key (userid, sessionid));

PARTITION TABLE user_usage_table ON COLUMN userid;

CREATE INDEX uut_ix1 ON user_usage_table(userid, lastdate);

create table user_recent_transactions
 MIGRATE TO TARGET user_transactions
(userid bigint not null 
,user_txn_id varchar(128) NOT NULL
,txn_time TIMESTAMP DEFAULT NOW  not null 
,sessionid bigint
,approved_amount bigint 
,spent_amount bigint 
,purpose  varchar(128)
,primary key (userid, user_txn_id));

PARTITION TABLE user_recent_transactions ON COLUMN userid;

CREATE INDEX urt_del_idx ON user_recent_transactions(userid, txn_time) ;

CREATE INDEX urt_del_idx2 ON user_recent_transactions(userid, txn_time)  WHERE NOT MIGRATING;

CREATE INDEX urt_del_idx3 ON user_recent_transactions(txn_time);

CREATE INDEX urt_del_idx4 ON user_recent_transactions(txn_time) WHERE NOT MIGRATING;

CREATE STREAM user_financial_events 
partition on column userid
export to target user_financial_events
(userid bigint not null 
,amount bigint not null
,user_txn_id varchar(128) not null
,message varchar(80) not null);

create view user_balance as
select userid, sum(amount) balance
from user_financial_events
group by userid;


create view cluster_activity_by_users as 
select userid,  count(*) how_many
from user_recent_transactions
group by userid;

create view cluster_activity as 
select truncate(minute, txn_time) txn_time, count(*) how_many
from user_recent_transactions
group by truncate(minute, txn_time) ;

create view last_cluster_activity as 
select  max(txn_time) txn_time
from user_recent_transactions;

create view cluster_users as 
select  count(*) how_many
from user_table;



create procedure showTransactions
PARTITION ON TABLE user_table COLUMN userid
as 
select * from user_recent_transactions where userid = ? ORDER BY txn_time, user_txn_id;


create procedure FindByLoyaltyCard as select * from user_table where field(user_json_object, 'loyaltySchemeNumber') = CAST(? AS VARCHAR);





CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.GetUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.GetAndLockUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.UpdateLockedUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.UpsertUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.DelUser;
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.ReportQuotaUsage;  
   
CREATE PROCEDURE 
   PARTITION ON TABLE user_table COLUMN userid
   FROM CLASS chargingdemoprocs.AddCredit;  


END_OF_BATCH
