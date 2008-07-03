#!/bin/sh
#
#  emageload.sh
###########################################################################
#
#  Purpose:
#
#      This script serves as a wrapper for the emageload.py script.
#
#  Usage:
#
#      emageload.sh
#
#
#  Env Vars:
#
#      See the configuration files
#
#  Inputs:  None
#
#  Outputs:
#
#      - Log file (${EMAGELOAD_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Create the temp table for the input data.
#      2) Call the Python script (emageload.py) to create a bcp file with
#         EMAGE associations and a discrepancy report for input records that
#         could not be processed.
#      3) Drop the temp table.
#      4) Delete the existing EMAGE associations.
#      5) Load the bcp file into the ACC_Accession table to establish the
#         new EMAGE associations.
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  07/07/2008  DBM  Initial development
#
###########################################################################

cd `dirname $0`
. ../Configuration

LOG=${EMAGELOAD_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

#
# Create a temporary file that will hold the return code from calling the
# Python script.  Make sure the file is removed when this script terminates.
#
TMP_RC=/tmp/`basename $0`.$$
trap "rm -f ${TMP_RC}" 0 1 2 15

#
# Create the temp table for the input data.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create the temp table (${EMAGE_TEMP_TABLE}) for the input data" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

create table ${EMAGE_TEMP_TABLE} (
    emageID varchar(80) not null,
    label varchar(80) not null,
    mgiID varchar(80) not null
)
go

create nonclustered index idx_emageID on ${EMAGE_TEMP_TABLE} (emageID)
go

create nonclustered index idx_label on ${EMAGE_TEMP_TABLE} (label)
go

create nonclustered index idx_mgiID on ${EMAGE_TEMP_TABLE} (mgiID)
go

grant all on ${EMAGE_TEMP_TABLE} to public
go

quit
EOSQL

#
# Create the EMAGE association file and discrepancy report.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create the EMAGE association file and discrepancy report" | tee -a ${LOG}
{ ./emageload.py 2>&1; echo $? > ${TMP_RC}; } >> ${LOG}
if [ `cat ${TMP_RC}` -ne 0 ]
then
    echo 'EMAGE load failed' | tee -a ${LOG}
    QUIT=1
elif [ ! -s ${EMAGELOAD_ACC_BCPFILE} ]
then
    echo 'The association file is empty' | tee -a ${LOG}
    QUIT=1
else
    QUIT=0
fi

#
# Drop the temp table.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Drop the temp table (${EMAGE_TEMP_TABLE})" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e  >> ${LOG}

use tempdb
go

drop table ${EMAGE_TEMP_TABLE}
go

quit
EOSQL

#
# Do not attempt to delete/reload the EMAGE associations if there was a
# problem creating the assocation file.
#
if [ ${QUIT} -eq 1 ]
then
    exit 1
fi

#
# Delete the existing EMAGE associations.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Delete the existing EMAGE associations" | tee -a ${LOG}
cat - <<EOSQL | isql -S${MGD_DBSERVER} -D${MGD_DBNAME} -Umgd_dbo -P`cat ${MGD_DBPASSWORDFILE}` -e >> ${LOG}

declare @logicalDBKey int
select @logicalDBKey = _LogicalDB_key
from ACC_LogicalDB
where name = 'EMAGE'

delete from ACC_Accession
where _LogicalDB_key = @logicalDBKey
go

quit
EOSQL

#
# Load the new EMAGE associations.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Load the new EMAGE associations" | tee -a ${LOG}
cat ${MGD_DBPASSWORDFILE} | bcp ${MGD_DBNAME}..ACC_Accession in ${EMAGELOAD_ACC_BCPFILE} -c -t\\t -S${MGD_DBSERVER} -U${MGD_DBUSER} >> ${LOG}

date >> ${LOG}

exit 0
