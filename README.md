# ETL-process

### About system:
Tables in SCD1 form, update insert and delete pull up from source to staging then to data warehouse after ETL, then system builds required reports (data mart). We can launch system by schedule, by example with help of cron.

ETL and OLAP Schematic:

<img src="ETL and OLAP Schematic.png">

scheme of data warehouse and staging area (actually tables have additional fields due to SCD1):

<img src="scheme.png">

