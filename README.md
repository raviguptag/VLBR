# VLBR
Vertica Logical Backup Recovery Script "VLBR.sh" has been created to archive data in open data formats like CSV ZIP , or PARQUET. 

## Use Cases where this utility can help
1) We have different Verica database environmets. Some of them are 3 node cluster and some of them are 6 node cluster. They cannot connect to each other to run IMPORT/EXPORT and we need an option to transfer data.
2) We have done a development on our 1 node database and we want to deploy our certain master data into different production clusters at different customer location and we want to give them dump so thay can deploy by themself. 
3) We want to backup certain tables data in open format to repurpose this OR feed into other applications as well. 


## Dependency & Assumptions 

1) This script will run on Vertica server as default database connection is pointed to localhost 
2) Default shell is /bin/bash
3) Vertica VSQL command is avalaible at /opt/vertica/bin/vsql
4) tar, gzip are installed. 
5) Vertica version is at least 8.1

## Examples 

1) Backup schema EDW into a target directory /home/dbadmin/bkp use below command  
$ VLBR.sh -t backup  -d /home/dbadmin/bkp -c PARQUET -u dbadmin -w password -s 

2) Backup certain tables - Firt define target tables in a text file like below and then use below command. In this example we want to backup 1 table store_fact in schema EDW and all tables of schema DIM.  

$ cat tables.txt

EDW.store_fact

DIM.*



$ VLBR.sh -t backup  -f tables.txt -d /home/dbadmin/bkp -c GZIP -u dbadmin -w password

3) Restore this backup data into another Vertica database. 
$ VLBR.sh -t restore -d /home/dbadmin/bkp -u dbadmin -w pass123

For more options use -h flag to get complete list.

## We need to be cautious   

1) If tables are really large like if individual table size is more than 5 GB then it is better we create PARQUET backup becuase CSV ZIP will create a single large file however parquet will create multiple files for single table's data.  
2) It cannot perform incremental Backup however Restore with -A will make it Append Only to load data into existing tables.
3) Currently password is provided as parameter but no one would like this. If we set VSQL_PASSWORD environmet variable in advance before runing VLBR.sh then no need to provide password and it will read password  from environment variable. 
