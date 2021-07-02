insert into ITDE1.MARS_REP_FRAUD(
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
where transactions.trans_id is not null and accounts.valid_to < sysdate;


select * from ITDE1.MARS_REP_FRAUD;

delete ITDE1.MARS_REP_FRAUD;
