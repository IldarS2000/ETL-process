-- Performing an incremental download
-- start of transaction
-------------------------------------

-- 1. Clearing data from STG
DELETE FROM ITDE1.MARS_STG_CLIENTS;
DELETE FROM ITDE1.MARS_STG_ACCOUNTS;
DELETE FROM ITDE1.MARS_STG_CARDS;
DELETE FROM ITDE1.MARS_STG_TERMINALS;
DELETE FROM ITDE1.MARS_STG_TRANSACTIONS;
DELETE FROM ITDE1.MARS_STG_PSSPRT_BLCKLST;

DELETE FROM ITDE1.MARS_STG_DEL_CLIENTS;
DELETE FROM ITDE1.MARS_STG_DEL_ACCOUNTS;
DELETE FROM ITDE1.MARS_STG_DEL_CARDS;

-- 2. Capturing data from source to STG
INSERT INTO ITDE1.MARS_STG_CLIENTS(
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
SELECT 
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
FROM BANK.CLIENTS
WHERE COALESCE(update_dt, create_dt) > (
	SELECT LAST_UPDATE FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_STG_CLIENTS'
);

INSERT INTO ITDE1.MARS_STG_ACCOUNTS(
    account_num,
    valid_to,
    client,
    create_dt,
    update_dt)
SELECT 
	account,
    valid_to,
    client,
    create_dt,
    update_dt
FROM BANK.ACCOUNTS
WHERE COALESCE(update_dt, create_dt) > (
	SELECT LAST_UPDATE FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_STG_ACCOUNTS'
);

INSERT INTO ITDE1.MARS_STG_CARDS(
    card_num,
    account_num,
    create_dt,
    update_dt)
SELECT 
    card_num,
    account,
    create_dt,
    update_dt
FROM BANK.CARDS
WHERE COALESCE(update_dt, create_dt) > (
	SELECT LAST_UPDATE FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_STG_CARDS'
);

-- 3. Updating the updated rows in the warehouse
-- Inserting facts
INSERT INTO ITDE1.MARS_DWH_FACT_TRANSACTIONS(
    trans_id,
    trans_date,
    card_num,
    oper_type,
    amt,
    oper_result,
    terminal,
    create_dt)
SELECT 
    trans_id,
    trans_date,
    card_num,
    oper_type,
    amt,
    oper_result,
    terminal,
    create_dt
FROM ITDE1.MARS_STG_TRANSACTIONS;

INSERT INTO ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST(
    passport_num,
    entry_dt,
    create_dt)
SELECT 
    passport_num,
    entry_dt,
    create_dt
FROM ITDE1.MARS_STG_PSSPRT_BLCKLST;

-- Loading dimensions
MERGE INTO ITDE1.MARS_DWH_DIM_CLIENTS tgt
USING ITDE1.MARS_STG_CLIENTS stg
ON (tgt.client_id = stg.client_id)
WHEN MATCHED THEN UPDATE SET
    tgt.last_name = stg.last_name,
    tgt.first_name = stg.first_name,
    tgt.patronymic = stg.patronymic,
    tgt.date_of_birth = stg.date_of_birth,
    tgt.passport_num = stg.passport_num,
    tgt.passport_valid_to = stg.passport_valid_to,
    tgt.phone = stg.phone,
    tgt.create_dt = stg.create_dt,
    tgt.update_dt = stg.update_dt
WHEN NOT MATCHED THEN INSERT(
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
) VALUES(
    stg.client_id,
    stg.last_name,
    stg.first_name,
    stg.patronymic,
    stg.date_of_birth,
    stg.passport_num,
    stg.passport_valid_to,
    stg.phone,
    stg.create_dt,
    stg.update_dt
);

MERGE INTO ITDE1.MARS_DWH_DIM_ACCOUNTS tgt
USING ITDE1.MARS_STG_ACCOUNTS stg
ON (tgt.account_num = stg.account_num)
WHEN MATCHED THEN UPDATE SET
    tgt.valid_to = stg.valid_to,
    tgt.client = stg.client,
    tgt.create_dt = stg.create_dt,
    tgt.update_dt = stg.update_dt
WHEN NOT MATCHED THEN INSERT(
    account_num,
    valid_to,
    client,
    create_dt,
    update_dt
) VALUES(
    stg.account_num,
    stg.valid_to,
    stg.client,
    stg.create_dt,
    stg.update_dt
);

MERGE INTO ITDE1.MARS_DWH_DIM_CARDS tgt
USING ITDE1.MARS_STG_CARDS stg
ON (tgt.card_num = stg.card_num)
WHEN MATCHED THEN UPDATE SET
    tgt.account_num = stg.account_num,
    tgt.create_dt = stg.create_dt,
    tgt.update_dt = stg.update_dt
WHEN NOT MATCHED THEN INSERT(
    card_num,
    account_num,
    create_dt,
    update_dt
) VALUES(
    stg.card_num,
    stg.account_num,
    stg.create_dt,
    stg.update_dt
);

MERGE INTO ITDE1.MARS_DWH_DIM_TERMINALS tgt
USING ITDE1.MARS_STG_TERMINALS stg
ON (tgt.terminal_id = stg.terminal_id)
WHEN MATCHED THEN UPDATE SET
    tgt.terminal_type = stg.terminal_type,
    tgt.terminal_city = stg.terminal_city,
    tgt.terminal_address = stg.terminal_address,
    tgt.create_dt = stg.create_dt,
    tgt.update_dt = stg.update_dt
WHEN NOT MATCHED THEN INSERT(
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    create_dt,
    update_dt
) VALUES(
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.create_dt,
    stg.update_dt
);

-- 4. Capturing keys to verify deletions
insert into ITDE1.MARS_STG_DEL_CLIENTS(id)
select client_id from BANK.CLIENTS;

insert into ITDE1.MARS_STG_DEL_ACCOUNTS(id)
select account from BANK.ACCOUNTS;

insert into ITDE1.MARS_STG_DEL_CARDS(id)
select card_num from BANK.CARDS;

-- 5. Delete deleted records in the target table
delete from ITDE1.MARS_DWH_DIM_CLIENTS clients
where clients.client_id in (
    select tgt.client_id
    from ITDE1.MARS_DWH_DIM_CLIENTS tgt
    left join ITDE1.MARS_STG_DEL_CLIENTS stg
    on tgt.client_id = stg.id
    where stg.id is null
);

delete from ITDE1.MARS_DWH_DIM_ACCOUNTS accounts
where accounts.account_num in (
    select tgt.account_num
    from ITDE1.MARS_DWH_DIM_ACCOUNTS tgt
    left join ITDE1.MARS_STG_DEL_ACCOUNTS stg
    on tgt.account_num = stg.id
    where stg.id is null
);

delete from ITDE1.MARS_DWH_DIM_CARDS cards
where cards.card_num in (
    select tgt.card_num
    from ITDE1.MARS_DWH_DIM_CARDS tgt
    left join ITDE1.MARS_STG_DEL_CARDS stg
    on tgt.card_num = stg.id
    where stg.id is null
);

-- 6. Updating metadata - maximum load date
UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_TRANSACTIONS )
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_FACT_TRANSACTIONS' 
	AND (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_TRANSACTIONS) IS NOT NULL;
    
UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_PSSPRT_BLCKLST )
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_FACT_PSSPRT_BLCKLST' 
	AND (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_PSSPRT_BLCKLST) IS NOT NULL;

UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CLIENTS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_CLIENTS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CLIENTS) IS NOT NULL;
    
UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_ACCOUNTS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_ACCOUNTS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_ACCOUNTS) IS NOT NULL;
    
UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CARDS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_CARDS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CARDS) IS NOT NULL;
    
UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_TERMINALS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_TERMINALS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_TERMINALS) IS NOT NULL;

-- 7. commit transaction
COMMIT;