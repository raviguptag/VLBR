#!/bin/bash 

# ########################################################################################################
# Name     : VLBR.sh
# Version  : 1.1
# Description:
#    1. This script is used to do logical backup of tables, schema data into open format like
#       CSV or parquet format, from Vertica dataabase
#    2. Script is Good to backup small or medium size databases and tables, but if they are large like 
#       more than 5 TB this script may run for quite long and will create really large files.  
#    3. This can not backup usres, roles, other objects. It can only backup tables data.
#    2. If we backup data through views then it can restored only same is available as table in target 
# Created by - Ravi Gupta, Vertica Systems
# ########################################################################################################

# Get values from parameter
# activity=$1
# TargetTablesIniFile=$2

# --  Set below parameter manually --------------------------------------------------------------------- #
VSQL_USER=dbadmin
VSQL_PASSWORD=password
COMPRESS_TYPE=GZIP
Port=5433
CompileBackupDir=0
BackupConcurrency=1
function usage() {
echo " 
Usage: VLBR.sh -t backup|restore -d backup_directory [-z GZIP|PARQUET]  [-f table_name_file] [-r resource_pool] -s [SchmeaName] [-A] [-F] [-h] 
    -t task             : task could be backup or restore 
    -d backup_directory : the backup directory path 
    -c compression      : compression type used for backup files. GZIP or PARQUET. Default GZIP 
    -f table_name_file  : Table names list file. format of the file as Schema.table_name in seperate line, or specifiy like Schema.*
    -r resource_pool    : Run backup under certain resource pool limitation. default pool general 
    -s Schema_Names     : Schema names delimited by , like Sch1,Sch2
    -F                  : Full Backup flag. if -F is used then -f and -s options are ignred
    -A                  : Append only flag. When used with restore option the data is loaded into exiting tables. If table not found then restore will fail 
    -C Concurrency      : Number of tables to be backup concurrently. Default it is 1 that mean one table at a time 
    -z abc.tar          : Compile the backup directory into a tar file 
    -h                  : Help Usage
"

}

VSQLA="/opt/vertica/bin/vsql -p $Port -t -A "
VSQL="/opt/vertica/bin/vsql -p $Port"

#echo "ECHO $# , , $* "

#---------------------------------------------------------------------------
# Command line options handling
#---------------------------------------------------------------------------
test $# -eq 0 && { usage; exit ; }

while [ $# -gt 0 ]; do
    case "$1" in
        "-r")
            RPOOL="SET SESSION RESOURCE POOL $2 ;"
            shift 2
            ;;
        "-d")
            BACKUP_DIR=$2
            mkdir -p $BACKUP_DIR
            # if [[ -d "$BACKUP_DIR" ]]; then mkdir -p "$BACKUP_DATA_DIR" ; else echo "Unable to get dir "; exit 1; fi; 
            test ! -d ${BACKUP_DIR} && { echo "ERROR: Cannot read directory ${BACKUP_DIR}"; exit 1; }
            shift 2
            ;;
        "-w")
            VSQL_PASSWORD=$2
            shift 2
            ;;
        "-t")
            TASK=$2
            shift 2
            ;;
        "-f")
            TableNamesFile=$2
            test ! -f ${TableNamesFile} && { echo "ERROR: Cannot read file ${TableNamesFile}"; usage; exit 1 ; }
            shift 2
            ;;
        "-u")
            VSQL_USER=$2
            shift 2
            ;;
        "-c")
            COMPRESS_TYPE=$2
            shift 2
            ;;
        "-C")
            BackupConcurrency=$2
            shift 2
            ;;
        "-s")
            SchemaList=$2
            shift 2
            ;;
        "-F")
            FullBackupRestore='Y'
            shift 1
            ;;
        "-A")
            AppendOnlyRestore='Y'
            shift 1
            ;;
        "-z")
            TarFile=$2
            CompileBackupDir=1
            shift 2
            ;;
        "--help" | "-h")
            usage;
            exit 0
            ;;
        *)
            echo "[VLBR] invalid option '$1'"
            usage;
            exit 1
            ;;
    esac
done


BackupCompletedFile=$BACKUP_DIR/VLBR_backup_tables.txt
BACKUP_DATA_DIR=${BACKUP_DIR}/data
BackupDir=$BACKUP_DIR
BackupDataDir=$BACKUP_DATA_DIR
BackupScriptsDir=${BackupDataDir}
BackupLog=${BackupDir}/backup.log
RestoreLog=${BackupDir}/restore.log
BackupTableListFile=${BackupDir}/backup_tables_list.txt
BackupTableDefinitions=${BackupDir}/backup_tables_ddl.sql

# -------------------------------------------------------------------
# Few Checks
# -------------------------------------------------------------------
testdb=`$VSQLA -c "SELECT 1"`
if [ $? -ne 0 ]; then echo "ERROR: Failed to connect database"; exit 1; fi

if [ "${SchemaList}X" != "X" ]; then 
  if [ -f "${TableNamesFile}" ]; then echo "ERROR: Cannot have Both Schema and Tables file, please check!"; exit 1; fi
fi
if [ "X${BACKUP_DIR}" == "X" ]; then 
  echo "ERROR: Backup directory not found. Please check."; usage; exit 1;
else 
  touch ${BACKUP_DIR}/test.file
  if [ $? -eq 0 ]; then rm ${BACKUP_DIR}/test.file; else echo "ERROR: Unable to read/write on director ${BACKUP_DIR}"; exit 1; fi
fi

function get_db_info() {
  $VSQL <<EOF >> $BackupLog 
  \! echo "-- Running Database Information -----------------------------------------------------"
  SELECT name from v_internal.vs_databases ;
  \x
  \! echo "System Table"
  SELECT * FROM system;
  \x
  SELECT node_name, node_state,node_address,catalog_path FROM nodes;
  \! echo "-------------------------------------------------------------------------------------"
EOF
}

function get_table_count() {
  tbl_cnt=`$VSQLA -c "SELECT count(1) FROM $*"`
  echo $tbl_cnt
}

function build_tables_list() {
  WHERE_CLAUSE=''
  if [ "${SchemaList}X" != "X" ]; then
    SchemaInClause=`echo $SchemaList | sed -e "s/,/','/g" -e "s/^/'/" -e "s/$/'/" -e "s/./\U&/g" `
  elif [ -f "$TableNamesFile" ]; then 
    SchemaInClause=`cat $TableNamesFile | cut -f1 -d'|'| grep '\.\*' | cut -f1 -d'.' | tr '\n' ',' | sed -e "s/,/','/g" -e "s/^/'/" -e "s/$/'/" -e "s/./\U&/g" `
  fi

  if [ "${FullBackupRestore}" == "Y" ]; then
    WHERE_CLAUSE=" WHERE is_flextable = 'f' ";
  else
    WHERE_CLAUSE=" WHERE upper(table_schema) IN ( $SchemaInClause ) AND is_flextable = 'f' "
  fi

  #echo "INFO: SchemaInClause = $SchemaInClause, and WHERE Clause is $WHERE_CLAUSE "
  $VSQLA <<EOF >> $BackupTableListFile 2>> $BackupLog
    SELECT table_schema||'.'||table_name 
      FROM tables
     $WHERE_CLAUSE ;
     -- WHERE table_name in ( 'emp', 'dept') -- please modify accordingly
     --  and table_schema in ('public');    -- please modify accordingly 
EOF
}

function backup() {
    # Check if need to backup target tables given by user in a ini file, OR pick tables from database using SQL
    # echo "INFO: Starting backup function with value of TableNamesFile =  $TableNamesFile";
    if [[ -f "$TableNamesFile" ]]; then 
        cat $TableNamesFile | grep -v '\.\*' > $BackupTableListFile
    fi  
    get_db_info >> $BackupLog 2>&1
    build_tables_list;
    rm -f ${BackupTableDefinitions}
    i=0
    touch $BackupCompletedFile
    echo "INFO: Total `wc -l $BackupTableListFile` tables will be logically Backup and list can be seen in file : $BackupCompletedFile" >> $BackupLog
    #for table_ask in `cat $BackupTableListFile`
    while read line
    do
        table=`echo $line      | cut -f1 -d'|'` 
        where_clause=`echo $line      | cut -f2 -d'|'`
        table_name=`echo $table | cut -f2 -d'.'`
        schema_name=`echo $table | cut -f1 -d'.'`
        if [[ "X${table}" == "X${where_clause}" ]]; then 
          where_clause=""; 
        fi
        rm -f $BackupDataDir/${table}.csv $BackupDataDir/${table}.csv.gz 
        echo "INFO: `date`: Starting backup of asked object $line , table =  $table , ${table_name}, Compress $COMPRESS_TYPE,  Where Clause = $where_clause"   >> $BackupLog
        table_count=`get_table_count $table`
        $VSQLA -c "SELECT export_objects('', '$table', false ); " | \
           sed -e 's/ OFFSET [01]//' -e 's/ KSAFE 1//' -e "s/\/\*+createtype/\/\*+basename($table_name),createtype/" -e "s/\*+basename(.*),crea/*+basename($var),crea/" >> $BackupTableDefinitions

        if [[ "$COMPRESS_TYPE" == "GZIP" ]]; then
            { $VSQLA -c "SELECT * FROM $table $where_clause" -o $BackupDataDir/${table}.csv >> $BackupLog; gzip $BackupDataDir/${table}.csv; } || { echo "ERROR: Failed to backup table $table"; exit 1; } &
            # gzip $BackupDataDir/${table}.csv &
        elif [[ "$COMPRESS_TYPE" == "PARQUET" ]]; then
            rm -rf $BackupDataDir/${table}/
            ExportSQL="EXPORT TO PARQUET ( directory='$BackupDataDir/${table}', rowGroupSizeMB=100 ) AS SELECT ";
            #ColumnStr=`$VSQLA -c "SELECT column_name||decode(data_type, 'time', '::varchar AS '||column_name||',', ',' )  FROM columns where table_schema = '$schema_name' and table_name = '$table_name'  order by ordinal_position" | tr -d '\n' | sed -e "s/,$/)/" -e "s/^/(/" `
            ColumnStr=`$VSQLA -c "SELECT column_name||decode(data_type, 'time', '::varchar AS '||column_name||',', ',' )  FROM columns where table_schema = '$schema_name' and table_name = '$table_name'  order by ordinal_position" | tr -d '\n' | sed -e "s/,$//" `
            # $VSQL -c "EXPORT TO PARQUET (directory = '$BackupDataDir/${table}') AS SELECT * FROM $table  $where_clause"
            # if [ $? -ne 0 ]; then echo "ERROR: Failed to backup table $table"; exit 1; fi
            #{ $VSQLA -c "EXPORT TO PARQUET (directory='$BackupDataDir/${table}', rowGroupSizeMB=100 ) AS SELECT * FROM $table  $where_clause"; } || { echo "ERROR: Export Parquet failed for $table" >> $BackupLog; exit 1; } &
            { $VSQLA -e -c "$ExportSQL $ColumnStr  FROM $table $where_clause" >> $BackupLog ; } || { echo "ERROR: Export Parquet failed for $table" >> $BackupLog; exit 1; } &
        else
            echo "ERROR: Unsupported compress type $COMPRESS_TYPE."; usage; exit 1;
        fi

        echo "INFO: `date`: Initialed backup of table $table where record count is $table_count"   >> $BackupLog
        echo "${table},${COMPRESS_TYPE},$table_count" >> $BackupCompletedFile
        ((i++))
        if [ $i -eq $BackupConcurrency ]; then 
            echo "INFO: `date`: Waiting for exports jobs to complete..."; 
            wait; 
            if [ `grep -c 'ERROR:' $BackupLog` -gt 0 ]; then exit 1; fi 
            i=0; 
        fi
    done < $BackupTableListFile

    wait;

    $VSQLA -c "SELECT export_catalog('$BackupDir/export_catalog.sql')";

    echo "INFO:`date`: Logical Backup completed" >> $BackupLog

}

function restore() {

    if [[ ! -f $BackupCompletedFile ]]; then 
        echo "ERROR: Unable to get table list file $BackupCompletedFile";exit 1;
    else
        echo "INFO: Total `wc -l $BackupCompletedFile` tables will be logically restored" >> $RestoreLog
    fi 
    cat $BackupCompletedFile | cut -f1 -d',' | cut -f1 -d'.' | sort -u | grep -v public | sed -e "s/^/CREATE SCHEMA /" -e "s/$/;/" > $BackupDir/VLBR_create_schema_list.sql
    cat $BackupCompletedFile | cut -f1 -d',' | sed -e "s/^/DROP TABLE /" -e "s/$/ CASCADE;/" > $BackupDir/VLBR_drop_tables.sql

    if [ "${FullBackupRestore}" == "Y" ]; then 
        $VSQLA -e -f $BackupDir/VLBR_drop_tables.sql >> $RestoreLog 2>&1
        $VSQLA    -f $BackupDir/export_catalog.sql   >> $RestoreLog 2>&1

    elif [ "$AppendOnlyRestore" == "Y" ]; then
        echo "INFO: Data will be loaded into exiting tables. if Any table not found then restore will fail";

    else
        if [[ ! -f $BackupTableDefinitions ]]; then 
            echo "WARNING: Unable to get table definitions $BackupTableDefinitions, COPY statements may fail" >>  $RestoreLog
        else
            $VSQLA -e -f $BackupDir/VLBR_create_schema_list.sql >> $RestoreLog 2>&1
            $VSQLA -e -f $BackupDir/VLBR_drop_tables.sql >> $RestoreLog 2>&1
            $VSQLA -e -f $BackupTableDefinitions         >>  $RestoreLog 2>&1
            if [ $? -ne 0 ]; then echo "ERROR: Failed to create tables"; exit 1; fi
        fi 
    fi

    conc_counter=0
    #for table in `cat $BackupTableListFile`
    while read line
    do
        table=`echo $line | cut -f1 -d','`;
        schema_name=`echo $table | cut -f1 -d'.'`;
        table_name=`echo $table | cut -f2 -d'.'`;
        COMPRESS_TYPE=`echo $line | cut -f2 -d','`;
        if [ $COMPRESS_TYPE == 'GZIP' ]; then
            data_file="${BackupDataDir}/${table}.csv.gz"
            if [[ -f $data_file ]]; then 
                echo "INFO: `date`: Starting copy for table $table_name, from file $data_file , Compression Type $COMPRESS_TYPE ." >> $RestoreLog
                $VSQLA -e -c " COPY $table FROM '$data_file'  $COMPRESS_TYPE DELIMITER '|' NULL AS '' DIRECT" >>  $RestoreLog 2>&1
                if [ $? -ne 0 ]; then echo "ERROR: Failed to restore table $table"; exit 1; fi
                echo "INFO: `date`: Completed restore of table $table"   >>  $RestoreLog
            else
                echo "ERROR: File $data_file not found for table $table"   >> $RestoreLog
            fi
        elif [ $COMPRESS_TYPE == 'PARQUET' ]; then
            data_dir="${BackupDataDir}/${table}"
            if [ ! -d $data_dir ]; then 
                echo "ERROR: Uanble to find data directory $data_dir"; exit 1; 
            else 
                flcnt=`ls -s "$data_dir" | head -1 | cut -f2 -d' '`;
                if [ $flcnt -gt 0 ]; then
                  ColumnStr=`$VSQLA -c "SELECT decode(data_type, 'time', ' _timeFiller FILLER VARCHAR, '||column_name||' AS _timeFiller::time,', column_name||',' )  FROM columns where table_schema = '$schema_name' and table_name = '$table_name'  order by ordinal_position" | tr -d '\n' | sed -e "s/^/(/" -e "s/,$/)/" `
                  for data_file in `ls $data_dir/*.parquet`
                  do
                     $VSQLA -e -c "COPY $table ${ColumnStr} FROM '$data_file' PARQUET " >> $RestoreLog 2>&1
                     if [ $? -ne 0 ]; then echo "ERROR: Failed to restore table $table_name "; exit 1; fi
                  done
                else
                   echo "INFO  : Data directory $data_dir for table $table_name found empty, ignore this" >> $RestoreLog
                fi
            fi
        fi
        echo "INFO:`date`:Table $table restored, Now record count is `get_table_count $table`" >> $RestoreLog
    done < $BackupCompletedFile

    echo "INFO: `date`: Logical Restore completed." >> $RestoreLog

}
activity=$TASK

# -- M A I N ------------------------------------------------------------------------- #

if [ $TASK == "backup" ]; then
    mkdir -p $BACKUP_DATA_DIR
    rm -f $BackupLog;
    echo "INFO: Starting Task $TASK at `date` on $BACKUP_DIR, with Target File = $TableNamesFile, Schmea = $SchemaList, Compression Type = $COMPRESS_TYPE "
    if [ -f "$TableNamesFile" ] || [ "X${SchemaList}" != "X" ] || [ "X${FullBackupRestore}" != "X" ];
    then
      rm -f $BackupCompletedFile
      backup;
    else
      echo "ERROR: When startig backup at least specifiy one parameter like -f Target table name file, -S schema name, OR -F for full bacup"
      usage;
      exit 1;   
    fi

elif [ $TASK == "restore" ]; then 
    echo "INFO: Starting Task $TASK at `date` on $BACKUP_DIR"
    if [ ! -f "$BackupCompletedFile" ]; then
        echo "ERROR: Unable to find file $BackupCompletedFile"; 
        exit 1;
    fi
    rm -f $RestoreLog;
    restore;

else
    echo -n "ERROR: Invalid Task option $TASK ! check usage. \n"; usage; 
    exit 1; 
fi

if [ $CompileBackupDir -gt 0 ]; 
then
    if [ -f "${TarFile}" ]; then rm -f ${TarFile}; fi
    bsdir=`dirname $BackupDir`
    bsname=`basename $BackupDir`
    cd $bsdir
    tar -cvf ${TarFile} $bsname/
fi

echo "INFO: At `date` , task $TASK  Done!!!!"
# -- D O N E ------------------------------------------------------------------------- #


