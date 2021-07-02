-- create staging tables
create table ITDE1.MARS_STG_DEL_CLIENTS(
	id varchar2(100) 
);

create table ITDE1.MARS_STG_DEL_ACCOUNTS(
	id varchar2(100) 
);

create table ITDE1.MARS_STG_DEL_CARDS(
	id varchar2(100) 
);

create table ITDE1.MARS_STG_CLIENTS(  
    client_id varchar2(100),
    last_name varchar2(100),
    first_name varchar2(100),
    patronymic varchar2(100),
    date_of_birth date,
    passport_num varchar2(100),
    passport_valid_to date,
    phone varchar2(100),
    create_dt date,
    update_dt date
);

create table ITDE1.MARS_STG_ACCOUNTS(  
    account_num varchar2(100),
    valid_to date,
    client varchar2(100),
    create_dt date,
    update_dt date
);

create table ITDE1.MARS_STG_CARDS(  
    card_num varchar2(100),
    account_num varchar2(100),
    create_dt date,
    update_dt date
);

create table ITDE1.MARS_STG_TERMINALS(  
    terminal_id varchar2(100),
    terminal_type varchar2(100),
    terminal_city varchar2(100),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

create table ITDE1.MARS_STG_TRANSACTIONS(  
    trans_id varchar2(100),
    trans_date date,
    card_num varchar2(100),
    oper_type varchar2(100),
    amt decimal(18, 2),
    oper_result varchar2(100),
    terminal varchar2(100),
    create_dt date
);

create table ITDE1.MARS_STG_PSSPRT_BLCKLST(  
    passport_num varchar2(100),
    entry_dt date,
    create_dt date
);

-- create target SCD1 tables
create table ITDE1.MARS_DWH_DIM_CLIENTS(  
    client_id varchar2(100),
    last_name varchar2(100),
    first_name varchar2(100),
    patronymic varchar2(100),
    date_of_birth date,
    passport_num varchar2(100),
    passport_valid_to date,
    phone varchar2(100),
    create_dt date,
    update_dt date
);

insert into ITDE1.MARS_DWH_DIM_CLIENTS(
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt)
select 
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt
from BANK.CLIENTS;

create table ITDE1.MARS_DWH_DIM_ACCOUNTS(  
    account_num varchar2(100),
    valid_to date,
    client varchar2(100),
    create_dt date,
    update_dt date
);

insert into ITDE1.MARS_DWH_DIM_ACCOUNTS(
    account_num,
    valid_to,
    client,
    create_dt,
    update_dt
)
select 
    account,
    valid_to,
    client,
    create_dt,
    update_dt
from BANK.ACCOUNTS;

create table ITDE1.MARS_DWH_DIM_CARDS(  
    card_num varchar2(100),
    account_num varchar2(100),
    create_dt date,
    update_dt date
);

insert into ITDE1.MARS_DWH_DIM_CARDS(
    card_num,
    account_num,
    create_dt,
    update_dt
)
select 
    card_num,
    account,
    create_dt,
    update_dt
from BANK.CARDS;

create table ITDE1.MARS_DWH_DIM_TERMINALS(  
    terminal_id varchar2(100),
    terminal_type varchar2(100),
    terminal_city varchar2(100),
    terminal_address varchar2(100),
    create_dt date,
    update_dt date
);

-- create fact tables
create table ITDE1.MARS_DWH_FACT_TRANSACTIONS(  
    trans_id varchar2(100),
    trans_date date,
    card_num varchar2(100),
    oper_type varchar2(100),
    amt decimal(18, 2),
    oper_result varchar2(100),
    terminal varchar2(100),
    create_dt date
);

create table ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST(  
    passport_num varchar2(100),
    entry_dt date,
    create_dt date
);

-- create meta table and insert into it
create table ITDE1.MARS_META_LOADING(
    db_name varchar2(100),
    table_name varchar2(100),
    last_update date
);

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_DIM_CLIENTS', to_date('1900-01-01', 'YYYY-MM-DD'));	

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_DIM_ACCOUNTS', to_date('1900-01-01', 'YYYY-MM-DD'));	

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_DIM_CARDS', to_date('1900-01-01', 'YYYY-MM-DD'));	

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_DIM_TERMINALS', to_date('1900-01-01', 'YYYY-MM-DD'));	

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_FACT_TRANSACTIONS', to_date('1900-01-01', 'YYYY-MM-DD'));	

insert into ITDE1.MARS_META_LOADING(db_name, table_name, last_update) 
values ('ITDE1', 'MARS_DWH_FACT_PSSPRT_BLCKLST', to_date('1900-01-01', 'YYYY-MM-DD'));	

-- create report table
create table ITDE1.MARS_REP_FRAUD(
    event_dt date,
    passport varchar2(100),
    fio varchar2(100),
    phone varchar2(100),
    event_type integer,
    report_dt date
);

-- commit changes
commit;