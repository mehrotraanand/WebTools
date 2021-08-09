"# WebTools" 
Web crawler to pull data from web pages into database table
Configuration: 
1. Pull WebTools directory into HOME directory
2. Make configuration changes in dref_master.ksh

Steps to run:
1. Create table using DDL/DREF_URL_EXTRACT_MASTER.ddl
2. Populate the table DREF_URL_EXTRACT_MASTER with required URL information, if need to test mark Active_flag = 99.
3. Create DB procedure to pull data from temporary table to final table. I used Oracle DB procedure, Any other 4GL could be used.
4. execute ~/WebTools/Crawler/dref_master.ksh
