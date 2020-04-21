# VLBR
Vertica Logical Backup Recovery Script "VLBR.sh" has been created to archive data in open data formats like CSV ZIP , or PARQUET. 

## Use Cases where this utility can help
1) We have different Verica database environmets. Some of them are 3 node cluster and some of them are 6 node cluster. They can not connect to each other to run IMPORT/EXPORT and we need an option to transfer data.
2) We have done a development on our 1 node database and we want to deploy our certain master data into different production clusters at different customer location and we want to give them dump so thay can deploy by themself. 
3) We want to backup certain tables data in open format to repurpose this OR feed into other applications as well. 


## Dependency & Assupitions 

1) Default shell is /bin/bash
2) Vertica VSQL command is avalaible at available at /opt/vertica/bin/vsql
3) This script will be runing on Vertica server as default database connection is on localhost 

## Examples 

1) Backup schema EDW into a target directory /home/dbadmin/bkp use below command  
$ VLBR.sh -t backup  -d /home/dbadmin/bkp -c PARQUET -u dbadmin -w password -s 

2) Backup certain tables - Firt define target tables in a text file like below and then use below command. In this example we want to backup 1 table store_fact in schema EDW and all tables of schema DIM.  

$ cat tables.txt

EDW.store_fact

DIM.*



$ VLBR.sh -t backup  -f tables.txt -d /home/dbadmin/bkp -c GZIP -u dbadmin -w password -s 

3) Restore this backup data into another Vertica database. 
$ VLBR.sh -t restore -d /home/dbadmin/bkp -u dbadmin -w pass123

For more options use -h flag to get complete list.
