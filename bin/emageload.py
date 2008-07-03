#!/usr/local/bin/python

#
#  emageload.py
###########################################################################
#
#  Purpose:
#
#      This script will create a bcp file for the ACC_Accession table
#      that contains associations between EMAGE IDs and image panes.
#
#  Usage:
#
#      emageload.py
#
#  Env Vars:
#
#      MGD_DBUSER
#      MGD_DBPASSWORDFILE
#      EMAGELOAD_INPUTFILE
#      EMAGELOAD_RPTFILE
#      EMAGELOAD_TEMP_BCPFILE
#      EMAGELOAD_ACC_BCPFILE
#      EMAGE_TEMP_TABLE
#      EMAGE_LOGICAL_DB
#      EMAGE_MGITYPE
#      EMAGE_CREATED_BY
#
#  Inputs:
#
#      emageListForGXD.csv (Tab-delimited fields)
#
#          1) EMAGE ID
#          2) Figure/pane label
#          3) MGI ID (for the assay)
#
#  Outputs:
#
#      BCP files:
#          emage_data.bcp
#          ACC_Accession.bcp
#
#      Discrepancy report:
#          emageload.rpt
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Assumes:  Nothing
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

import sys
import os
import string
import re
import accessionlib
import loadlib
import mgi_utils
import db

#
#  CONSTANTS
#
TAB = '\t'
NL = '\n'

PRIVATE = '0'
PREFERRED = '1'

#
#  GLOBALS
#
user = os.environ['MGD_DBUSER']
passwordFile = os.environ['MGD_DBPASSWORDFILE']
inputFile = os.environ['EMAGELOAD_INPUTFILE']
rptFile = os.environ['EMAGELOAD_RPTFILE']
tempBCPFile = os.environ['EMAGELOAD_TEMP_BCPFILE']
accBCPFile = os.environ['EMAGELOAD_ACC_BCPFILE']

tempTable = os.environ['EMAGE_TEMP_TABLE']
logicalDB = os.environ['EMAGE_LOGICAL_DB']
mgiType = os.environ['EMAGE_MGITYPE']
createdBy = os.environ['EMAGE_CREATED_BY']

loadDate = loadlib.loaddate
timestamp = mgi_utils.date()


#
# Purpose: Perform initialization steps.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def init ():
    global accKey, logicalDBKey, MGITypeKey, createdByKey

    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFile)

    #
    # Get the keys from the database.
    #
    cmds = []
    cmds.append('select max(_Accession_key) + 1 "_Accession_key" from ACC_Accession')

    cmds.append('select _LogicalDB_key from ACC_LogicalDB ' + \
                'where name = "%s"' % (logicalDB))

    cmds.append('select _MGIType_key from ACC_MGIType ' + \
                'where name = "%s"' % (mgiType))

    cmds.append('select _User_key from MGI_User ' + \
                'where name = "%s"' % (createdBy))

    results = db.sql(cmds,'auto')

    #
    # If any of the keys cannot be found, stop the load.
    #
    if len(results[0]) == 1:
        accKey = results[0][0]['_Accession_key']
    else:
        print 'Cannot determine the next Accession key'
        sys.exit(1)

    if len(results[1]) == 1:
        logicalDBKey = results[1][0]['_LogicalDB_key']
    else:
        print 'Cannot determine the Logical DB key for "' + logicalDB + '"'
        sys.exit(1)

    if len(results[2]) == 1:
        MGITypeKey = results[2][0]['_MGIType_key']
    else:
        print 'Cannot determine the MGI Type key for "' + mgiType + '"'
        sys.exit(1)

    if len(results[3]) == 1:
        createdByKey = results[3][0]['_User_key']
    else:
        print 'Cannot determine the User key for "' + createdBy + '"'
        sys.exit(1)

    return


#
# Purpose: Open the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInputFile, fpRptFile, fpAccBCPFile

    #
    # Open the input file.
    #
    try:
        fpInputFile = open(inputFile, 'r')
    except:
        print 'Cannot open input file: ' + inputFile
        sys.exit(1)

    #
    # Open the report file.
    #
    try:
        fpRptFile = open(rptFile, 'w')
    except:
        print 'Cannot open report file: ' + rptFile
        sys.exit(1)

    #
    # Open the output file.
    #
    try:
        fpAccBCPFile = open(accBCPFile, 'w')
    except:
        print 'Cannot open output file: ' + accBCPFile
        sys.exit(1)

    return


#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles ():
    fpInputFile.close()
    fpRptFile.close()
    fpAccBCPFile.close()

    return


#
# Purpose: Load the data from the input file into the temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def loadTempTable ():

    #
    # Open the bcp file.
    #
    try:
        fpTempBCPFile = open(tempBCPFile, 'w')
    except:
        print 'Cannot open bcp file: ' + tempBCPFile
        sys.exit(1)

    #
    # Read each record from the input file, validate the fields and write
    # them to a bcp file.
    #
    line = fpInputFile.readline()
    count = 1
    while line:
        tokens = re.split(TAB, line[:-1])
        emageID = tokens[0]
        label = tokens[1]
        mgiID = tokens[2]

        if len(emageID) == 0 or len(label) == 0 or len(mgiID) == 0:
            print 'Invalid input record (line ' + str(count) + ')'
            sys.exit(1)

        fpTempBCPFile.write(emageID + TAB + label + TAB + mgiID + NL)

        line = fpInputFile.readline()
        count = 1

    #
    # Close the bcp file.
    #
    fpTempBCPFile.close()

    #
    # Load the temp table with the input data.
    #
    print 'Load the temp table with the input data'
    sys.stdout.flush()

    bcpCmd = 'cat %s | bcp tempdb..%s in %s -c -t"%s" -S%s -U%s' % (passwordFile, tempTable, tempBCPFile, TAB, db.get_sqlServer(), db.get_sqlUser())
    os.system(bcpCmd)

    return


#
# Purpose: Create the discrepancy report for the EMAGE data.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createReport ():
    print 'Create the discrepancy report'
    fpRptFile.write(25*' ' + 'EMAGE Discrepancy Report' + NL)
    fpRptFile.write(24*' ' + '(' + timestamp + ')' + 2*NL)
    fpRptFile.write('%-12s  %-12s  %-25s  %-40s%s' %
                   ('EMAGE ID','Assay','Figure/Pane Label','Discrepancy',NL))
    fpRptFile.write(12*'-' + '  ' + 12*'-' + '  ' + 25*'-' + '  ' + \
                    40*'-' + NL)

    cmds = []

    #
    # Find any cases where the MGI ID from the input file does not represent
    # an assay.
    #
    cmds.append('select t.emageID, t.mgiID, t.label ' + \
                'from tempdb..' + tempTable + ' t ' + \
                'where not exists (select 1 ' + \
                                  'from ACC_Accession a ' + \
                                  'where t.mgiID = a.accID and ' + \
                                        'a._MGIType_key = 8) ' + \
                'order by t.emageID, t.mgiID, t.label')

    #
    # Find any cases where the figure/pane label from the input file does not
    # exist for the assay.
    #
    cmds.append('select t.emageID, t.mgiID, t.label ' + \
                'from tempdb..' + tempTable + ' t, ACC_Accession a1 ' + \
                'where t.mgiID = a1.accID and ' + \
                      'not exists (select 1 ' + \
                              'from ACC_Accession a2, GXD_Specimen s, ' + \
                                   'GXD_InSituResult r, ' + \
                                   'GXD_InSituResultImage ri, ' + \
                                   'IMG_ImagePane ip, IMG_Image i ' + \
                              'where t.mgiID = a2.accID and ' + \
                                    'a2._MGIType_key = 8 and ' + \
                                    'a2._Object_key = s._Assay_key and ' + \
                                    's._Specimen_key = r._Specimen_key and ' + \
                                    'r._Result_key = ri._Result_key and ' + \
                                    'ri._ImagePane_key = ip._ImagePane_key and ' + \
                                    'ip._Image_key = i._Image_key and ' + \
                                    'i.figureLabel + ip.paneLabel = t.label) ' + \
                'order by t.emageID, t.mgiID, t.label')

    results = db.sql(cmds,'auto')

    #
    # Write the records to the discrepancy report.
    #
    for r in results[0]:
        fpRptFile.write('%-12s  %-12s  %-25s  %-40s%s' %
            (r['emageID'], r['mgiID'], r['label'], 'MGI ID does not exist for an assay', NL))

    for r in results[1]:
        fpRptFile.write('%-12s  %-12s  %-25s  %-40s%s' %
            (r['emageID'], r['mgiID'], r['label'], 'Invalid figure/pane label for the assay', NL))

    fpRptFile.write(NL + 'Number of discrepancies: ' + \
                    str(len(results[0]) + len(results[1])) + NL)

    print 'Number of discrepancies: ' + str(len(results[0]) + len(results[1]))

    return


#
# Purpose: Create the bcp file for the EMAGE associations.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def createBCPFile ():
    global accKey

    print 'Create the bcp file for the EMAGE associations'

    #
    # Find the image pane key for the figure/pane label for each assay from
    # the input file.
    #
    cmds = []
    cmds.append('select distinct t.emageID, ip._ImagePane_key, ' + \
                       'i.figureLabel, ip.paneLabel, t.label ' + \
                'from tempdb..' + tempTable + ' t, ACC_Accession a, ' + \
                     'GXD_Specimen s, GXD_InSituResult r, ' + \
                     'GXD_InSituResultImage ri, ' + \
                     'IMG_ImagePane ip, IMG_Image i ' + \
                'where t.mgiID = a.accID and ' + \
                      'a._MGIType_key = 8 and ' + \
                      'a._Object_key = s._Assay_key and ' + \
                      's._Specimen_key = r._Specimen_key and ' + \
                      'r._Result_key = ri._Result_key and ' + \
                      'ri._ImagePane_key = ip._ImagePane_key and ' + \
                      'ip._Image_key = i._Image_key and ' + \
                      'i.figureLabel + ip.paneLabel = t.label ' + \
                'order by t.emageID, ip._ImagePane_key')

    results = db.sql(cmds,'auto')

    count = 0

    #
    # Write the records to the bcp file.
    #
    for r in results[0]:
        emageID = r['emageID']
        imagePaneKey = r['_ImagePane_key']
        figureLabel = r['figureLabel']
        paneLabel = r['paneLabel']
        label = r['label']

        if paneLabel == None:
            paneLabel = ''

        #
        # Sybase does not do a case-sensitive search, so we need to make
        # sure that the labels are the same on the Python side and skip the
        # ones that don't match.  For example, "1" + "A" != "1a".
        #
        if figureLabel + paneLabel != label:
            continue

        (prefixPart,numericPart) = accessionlib.split_accnum(emageID)

        fpAccBCPFile.write(str(accKey) + TAB + \
                           emageID + TAB + \
                           prefixPart + TAB + \
                           str(numericPart) + TAB + \
                           str(logicalDBKey) + TAB + \
                           str(imagePaneKey) + TAB + \
                           str(MGITypeKey) + TAB + \
                           PRIVATE + TAB + PREFERRED + TAB + \
                           str(createdByKey) + TAB + \
                           str(createdByKey) + TAB + \
                           loadDate + TAB + \
                           loadDate + NL)

        count = count + 1
        accKey = accKey + 1

    print 'Number of EMAGE associations: ' + str(count)

    return


#
# Main
#
init()
openFiles()
loadTempTable()
createReport()
createBCPFile()
closeFiles()

sys.exit(0)
