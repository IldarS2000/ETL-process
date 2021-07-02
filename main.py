import jaydebeapi
import pandas as pd
import re
from datetime import datetime
import shutil
import os


def move_file_to_archive(filename):
    shutil.move(f'{filename}', f'archive/{filename}')


def find_file_with_min_date(filenames):
    func = lambda filename: datetime.strptime(re.search('(\d{8})\.', filename).group(1), '%d%m%Y')
    return min(filenames, key=func)


def get_filenames():
    listdir = os.listdir()

    blacklist_filenames = list(filter(lambda file: file.startswith('passport_blacklist'), listdir))
    blacklist_filename = find_file_with_min_date(blacklist_filenames)

    terminals_filenames = list(filter(lambda file: file.startswith('terminals'), listdir))
    terminals_filename = find_file_with_min_date(terminals_filenames)

    transactions_filenames = list(filter(lambda file: file.startswith('transactions'), listdir))
    transactions_filename = find_file_with_min_date(transactions_filenames)

    return blacklist_filename, terminals_filename, transactions_filename


blacklist_filename, terminals_filename, transactions_filename = get_filenames()


def read_records_from_txt(filename):
    return pd.read_csv(filename, delimiter=';')


def read_records_from_xlsx(filename):
    return pd.read_excel(filename)


def clear_staging(cursor: jaydebeapi.Cursor):
    cursor.execute("DELETE FROM ITDE1.MARS_STG_CLIENTS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_ACCOUNTS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_CARDS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_TERMINALS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_TRANSACTIONS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_PSSPRT_BLCKLST")

    cursor.execute("DELETE FROM ITDE1.MARS_STG_DEL_CLIENTS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_DEL_ACCOUNTS")
    cursor.execute("DELETE FROM ITDE1.MARS_STG_DEL_CARDS")


def capture_data_from_source_to_staging(cursor: jaydebeapi.Cursor):
    cursor.execute("""INSERT INTO ITDE1.MARS_STG_CLIENTS(
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
	SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_DIM_CLIENTS'
)""")

    cursor.execute("""INSERT INTO ITDE1.MARS_STG_ACCOUNTS(
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
	SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_DIM_ACCOUNTS'
)""")

    cursor.execute("""INSERT INTO ITDE1.MARS_STG_CARDS(
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
	SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_DIM_CARDS'
)""")

    df = read_records_from_xlsx(terminals_filename)
    for index, row in enumerate(df.values):
        cursor.execute("""insert into ITDE1.MARS_STG_TERMINALS(
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                create_dt)
        select ?, ?, ?, ?, sysdate from dual
        WHERE sysdate > (
        	SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_DIM_TERMINALS'
        )""", tuple(row))

    df = read_records_from_xlsx(blacklist_filename)
    for index, row in enumerate(df.values):
        cursor.execute("""insert into ITDE1.MARS_STG_PSSPRT_BLCKLST(
                passport_num,
                entry_dt,
                create_dt)
        select ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'), sysdate from dual
        WHERE sysdate > (
        	SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_FACT_PSSPRT_BLCKLST'
        )""", (row[1], str(row[0])))

    data = read_records_from_txt(transactions_filename)
    for index, row in enumerate(data.values):
        cursor.execute("""insert into ITDE1.MARS_STG_TRANSACTIONS(
        trans_id,
        trans_date,
        card_num,
        oper_type,
        amt,
        oper_result,
        terminal,
        create_dt)
    select ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?, sysdate from dual
    WHERE sysdate > (
        SELECT last_update FROM ITDE1.MARS_META_LOADING WHERE db_name = 'ITDE1' AND table_name = 'MARS_DWH_FACT_TRANSACTIONS'
    )""", (row[0], row[1], row[3], row[4], row[2], row[5], row[6]))
        if index % 100 == 0:
            print(index)


def update_updated_rows_warehouse(cursor: jaydebeapi.Cursor):
    cursor.execute("""INSERT INTO ITDE1.MARS_DWH_FACT_TRANSACTIONS(
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
FROM ITDE1.MARS_STG_TRANSACTIONS""")

    cursor.execute("""INSERT INTO ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST(
    passport_num,
    entry_dt,
    create_dt)
SELECT 
    passport_num,
    entry_dt,
    create_dt
FROM ITDE1.MARS_STG_PSSPRT_BLCKLST""")

    cursor.execute("""MERGE INTO ITDE1.MARS_DWH_DIM_CLIENTS tgt
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
)""")

    cursor.execute("""MERGE INTO ITDE1.MARS_DWH_DIM_ACCOUNTS tgt
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
)""")

    cursor.execute("""MERGE INTO ITDE1.MARS_DWH_DIM_CARDS tgt
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
)""")

    cursor.execute("""MERGE INTO ITDE1.MARS_DWH_DIM_TERMINALS tgt
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
)""")


def capture_keys_to_verify_deletions(cursor: jaydebeapi.Cursor):
    cursor.execute("""insert into ITDE1.MARS_STG_DEL_CLIENTS(id)
select client_id from BANK.CLIENTS""")
    cursor.execute("""insert into ITDE1.MARS_STG_DEL_ACCOUNTS(id)
select account from BANK.ACCOUNTS""")
    cursor.execute("""insert into ITDE1.MARS_STG_DEL_CARDS(id)
select card_num from BANK.CARDS""")


def delete_deleted_records_in_the_target_table(cursor: jaydebeapi.Cursor):
    cursor.execute("""delete from ITDE1.MARS_DWH_DIM_CLIENTS clients
where clients.client_id in (
    select tgt.client_id
    from ITDE1.MARS_DWH_DIM_CLIENTS tgt
    left join ITDE1.MARS_STG_DEL_CLIENTS stg
    on tgt.client_id = stg.id
    where stg.id is null
)""")
    cursor.execute("""delete from ITDE1.MARS_DWH_DIM_ACCOUNTS accounts
where accounts.account_num in (
    select tgt.account_num
    from ITDE1.MARS_DWH_DIM_ACCOUNTS tgt
    left join ITDE1.MARS_STG_DEL_ACCOUNTS stg
    on tgt.account_num = stg.id
    where stg.id is null
)""")
    cursor.execute("""delete from ITDE1.MARS_DWH_DIM_CARDS cards
where cards.card_num in (
    select tgt.card_num
    from ITDE1.MARS_DWH_DIM_CARDS tgt
    left join ITDE1.MARS_STG_DEL_CARDS stg
    on tgt.card_num = stg.id
    where stg.id is null
)""")


def update_metadata(cursor: jaydebeapi.Cursor):
    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_TRANSACTIONS )
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_FACT_TRANSACTIONS' 
	AND (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_TRANSACTIONS) IS NOT NULL""")

    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_PSSPRT_BLCKLST )
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_FACT_PSSPRT_BLCKLST' 
	AND (SELECT MAX(create_dt) FROM ITDE1.MARS_STG_PSSPRT_BLCKLST) IS NOT NULL""")

    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CLIENTS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_CLIENTS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CLIENTS) IS NOT NULL""")

    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_ACCOUNTS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_ACCOUNTS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_ACCOUNTS) IS NOT NULL""")

    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CARDS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_CARDS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_CARDS) IS NOT NULL""")

    cursor.execute("""UPDATE ITDE1.MARS_META_LOADING
SET last_update = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_TERMINALS)
WHERE 1=1
	AND db_name = 'ITDE1' 
	AND table_name = 'MARS_DWH_DIM_TERMINALS' 
	AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM ITDE1.MARS_STG_TERMINALS) IS NOT NULL""")


def execute_increment(cursor: jaydebeapi.Cursor):
    clear_staging(cursor)
    print('clear_staging: ok')

    capture_data_from_source_to_staging(cursor)
    print('capture_data_from_source_to_staging: ok')

    update_updated_rows_warehouse(cursor)
    print('update_updated_rows_warehouse: ok')

    capture_keys_to_verify_deletions(cursor)
    print('capture_keys_to_verify_deletions: ok')

    delete_deleted_records_in_the_target_table(cursor)
    print('delete_deleted_records_in_the_target_table: ok')

    update_metadata(cursor)
    print('update_metadata: ok')

    print('increment executed: ok')


def build_report(cursor: jaydebeapi.Cursor):
    cursor.execute("""insert into ITDE1.MARS_REP_FRAUD(
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt)
select 
    transactions.trans_date event_dt, 
    clients.passport_num passport,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic fio,
    clients.phone phone,
    1 event_type,
    sysdate report_dt
from ITDE1.MARS_DWH_DIM_CLIENTS clients 
    left join ITDE1.MARS_DWH_DIM_ACCOUNTS accounts on clients.client_id = accounts.client
    left join ITDE1.MARS_DWH_DIM_CARDS cards on accounts.account_num = cards.account_num
    left join ITDE1.MARS_DWH_FACT_TRANSACTIONS transactions on transactions.card_num = cards.card_num
where transactions.trans_id is not null and (clients.passport_valid_to < sysdate or 
    clients.passport_num in (select blacklist.passport_num from ITDE1.MARS_DWH_FACT_PSSPRT_BLCKLST blacklist))
union
select 
    transactions.trans_date event_dt, 
    clients.passport_num passport,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic fio,
    clients.phone phone,
    2 event_type,
    sysdate report_dt
from ITDE1.MARS_DWH_DIM_CLIENTS clients 
    left join ITDE1.MARS_DWH_DIM_ACCOUNTS accounts on clients.client_id = accounts.client
    left join ITDE1.MARS_DWH_DIM_CARDS cards on accounts.account_num = cards.account_num
    left join ITDE1.MARS_DWH_FACT_TRANSACTIONS transactions on transactions.card_num = cards.card_num
where transactions.trans_id is not null and accounts.valid_to < sysdate""")
    print('build_report: ok')


def main():
    connection = jaydebeapi.connect(
        'oracle.jdbc.driver.OracleDriver',
        'jdbc:oracle:thin:host:port/sid',
        ['username', 'password'],
        'ojdbc8.jar'
    )
    connection.jconn.setAutoCommit(False)
    cursor = connection.cursor()

    execute_increment(cursor)
    connection.commit()

    build_report(cursor)
    connection.commit()

    cursor.close()
    connection.close()

    move_file_to_archive(blacklist_filename)
    move_file_to_archive(terminals_filename)
    move_file_to_archive(transactions_filename)


if __name__ == '__main__':
    main()
