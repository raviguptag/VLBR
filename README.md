# VLBR
Vertica Logical Backup Recovery script to archive data in open data formats

## Dependency & Assupitions 

1) Default shell is /bin/bash
2) Vertica VSQL command is avalaible at available at /opt/vertica/bin/vsql
3) This script will be runing on Vertica server as default database connection is on localhost 

## Examples 

1) Backup schema EDW into a target directory /home/dbadmin/bkp use below command  
$ VLBR.sh -t backup  -d /home/dbadmin/bkp -c PARQUET -u dbadmin -w password -s 

2) Backup certain tables on a backup target directory /home/dbadmin/bkp use below command  
$ VLBR.sh -t backup  -d /home/dbadmin/bkp -c PARQUET -u dbadmin -w password -s 


3) Restore this backup data into another Vertica database. 
$ VLBR.sh -t restore -d /home/dbadmin/bkp -u dbadmin -w pass123
