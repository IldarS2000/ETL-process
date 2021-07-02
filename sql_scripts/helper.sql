ALTER SESSION SET nls_date_format='YYYY-MM-DD';

-- drop all tables
drop table ITDE1.MARS_STG_DEL_CLIENTS;
drop table ITDE1.MARS_STG_DEL_ACCOUNTS;
drop table ITDE1.MARS_STG_DEL_CARDS;

drop table ITDE1.MARS_STG_CLIENTS;
drop table ITDE1.MARS_STG_ACCOUNTS;
drop table ITDE1.MARS_STG_CARDS;
drop table ITDE1.MARS_STG_TERMINALS;
drop table ITDE1.MARS_STG_TRANSACTIONS;
drop table ITDE1.MARS_STG_PSSPRT_BLCKLST;

drop table ITDE1.MARS_DWH_DIM_CLIENTS;
drop table ITDE1.MARS_DWH_DIM_ACCOUNTS;
drop table ITDE1.MARS_DWH_DIM_CARDS;
drop table ITDE1.MARS_DWH_DIM_TERMINALS;
drop table ITDE1.MARS_DWH_FACT_TRANSACTIONS;
drop table ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST;

drop table ITDE1.MARS_META_LOADING;
drop table ITDE1.MARS_REP_FRAUD;

commit;

-- selects from tables
select * from ITDE1.MARS_STG_DEL_CLIENTS;
select * from ITDE1.MARS_STG_DEL_ACCOUNTS;
select * from ITDE1.MARS_STG_DEL_CARDS;

select * from ITDE1.MARS_STG_CLIENTS;
select * from ITDE1.MARS_STG_ACCOUNTS;
select * from ITDE1.MARS_STG_CARDS;
select * from ITDE1.MARS_STG_TERMINALS;
select * from ITDE1.MARS_STG_TRANSACTIONS;
select * from ITDE1.MARS_STG_PSSPRT_BLCKLST;

select * from ITDE1.MARS_DWH_DIM_CLIENTS;
select * from ITDE1.MARS_DWH_DIM_ACCOUNTS;
select * from ITDE1.MARS_DWH_DIM_CARDS;
select * from ITDE1.MARS_DWH_DIM_TERMINALS;
select * from ITDE1.MARS_DWH_FACT_TRANSACTIONS;
select * from ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST;

select * from ITDE1.MARS_META_LOADING;
select * from ITDE1.MARS_REP_FRAUD;