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
#  Inputs:
#
#      - Input file (${EMAGELOAD_INPUTFILE}) with the following
#        tab-delimited fields:
#
#          1) EMAGE ID
#          2) Figure/pane label
#          3) MGI ID (for the assay)
#
#  Outputs:
#
#      - Translated Input File (${EMAGELOAD_LOAD_INPUTFILE})
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
#      1) Remove any duplicate records from the input file and remove the
#         text 'Figure ' from the figure label (e.g. 'Figure 1A' --> '1A').
#      2) Create the temp table for the input data.
#      3) Call the Python script (emageload.py) to create a bcp file with
#         EMAGE associations and a discrepancy report for input records that
#         could not be processed.
#      4) Drop the temp table.
#      5) Delete the existing EMAGE associations.
#      6) Load the bcp file into the ACC_Accession table to establish the
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
# Make sure the input file exists.  Perform a unique sort on it to remove
# any possible duplicates and remove the 'Figure ' text for the figure label.
# This text is not stored in the database, so it would fail on join
# conditions.  The translated file will be used as input by the load.
#
if [ ! -f ${EMAGELOAD_INPUTFILE} ]
then
    echo "Missing input file: ${EMAGELOAD_INPUTFILE}" | tee -a ${LOG}
    exit 1
fi
rm -f ${EMAGELOAD_LOAD_INPUTFILE}
sort -u ${EMAGELOAD_INPUTFILE} | sed 's/Figure //' > ${EMAGELOAD_LOAD_INPUTFILE}

#
# Create the temp table for the input data.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Create the temp table (${EMAGE_TEMP_TABLE}) for the input data" | tee -a ${LOG}
cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0 | tee -a ${LOG}

drop table if exists radar.${EMAGE_TEMP_TABLE}
;

CREATE TABLE radar.${EMAGE_TEMP_TABLE} (
    emageID text not null,
    label text not null,
    mgiID text not null
)
;

create index idx_emageID on radar.${EMAGE_TEMP_TABLE} (emageID)
;

create index idx_label on radar.${EMAGE_TEMP_TABLE} (label)
;

create index idx_mgiID on radar.${EMAGE_TEMP_TABLE} (mgiID)
;

grant all on radar.${EMAGE_TEMP_TABLE} to public
;

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
    echo "EMAGE load failed" | tee -a ${LOG}
    QUIT=1
elif [ ! -s ${EMAGELOAD_ACC_BCPFILE} ]
then
    echo "The association file is empty" | tee -a ${LOG}
    QUIT=1
else
    QUIT=0
fi

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
cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0 | tee -a ${LOG}

delete from ACC_Accession where _LogicalDB_key = 116
;

EOSQL

#
# Load the new EMAGE associations.
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Load the new EMAGE associations" | tee -a ${LOG}
${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ACC_Accession ${EMAGELOAD_OUTPUTDIR} ACC_Accession.bcp "\t" "\n" mgd

date >> ${LOG}

exit 0
