#!/opt/python3.7/bin/python3
#  preprocess.py
###########################################################################
#
#  Purpose:
#
#      This script will parse input files to
#      create a MP/HP mapping (relationship) load file
#
#  Usage:
#      preprocess.py
#
#  Env Vars:
#	See the configuration file (mp_hpmappingload.config)
#
#  Inputs:
#     The set of files specified by INPUT_FILE_NAMES and found in
#       DOWNLOAD_DIR
#
#  Outputs:
#
#   intermediate file INPUT_FILE_TOLOAD  Format: 
#   1. MP ID - relationship organizer
#   2. HP ID - relationship participant
#   3. Predicate ID - property
#   4. Mapping Justification - property
#   5. Input file name - property
#    
#  Exit Codes:
#      0:  Successful completion
#      1:  An exception occurred
#
#  Implementation:
#      This script will perform following steps:
#       1) initialize - get values from the environment, load lookups
#	2) open input/output files
#	3) parse input files to create intermediate file, do QC
#	4) close input/output files
#
#  Notes: 
#
#  11/18/2022	sc
#	- TR11674
#
###########################################################################

import sys 
import os
import string
import Set
import db
import time

#db.setTrace(True)

CRT = '\n'
TAB = '\t'

# Outputs 
inputFileInt = None
logDiagFile = None
logCurFile = None

# file pointers
fpInput = None # will be used over and over for each file
fpInputInt = None
fpLogDiag = None
fpLogCur = None

# value for blank predicate and justification
unspecified = 'unspecified'

downloadDir = os.getenv('DOWNLOAD_DIR')
predicateIncludeList = str.split(os.getenv('PREDICATES_TO_LOAD'), ', ')

# Lookups
# {mpID:key, ...}
mpDict = {}

# {hpID:key, ...}
hpDict = {}

#
# Purpose: Initialization  of variable with values from the environment
#	load lookup structures from the database
# Returns: 1 if environment variable not set
# Assumes: Nothing
# Effects: opens a database connection
# Throws: Nothing
#
def initialize():
    global inputFileInt, logDiagFile, logCurFile
    global mpDict, hpDict

    inputFileInt = os.getenv('INPUT_FILE_TOLOAD')
    logDiagFile = os.getenv('LOG_DIAG')
    logCurFile = os.getenv('LOG_CUR')

    db.useOneConnection(1)

    # lookup of preferred MP IDs/terms
    results = db.sql('''select a.accid, a._object_key
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 34
        and a.preferred = 1
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        mpDict[r['accid']] = r['_object_key']

    # lookup of preferred HP IDs/terms
    results = db.sql('''select a.accid, a._object_key
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 180
        --and a.preferred = 1
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        hpDict[r['accid']] = r['_object_key']

    return 0

#
# Purpose: Open input/output files.
# Returns: 1 if file does not exist or is not readable, else 0
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#

def openFiles():
    global fpInputInt, fpLogDiag, fpLogCur

    #
    # Open the intermediate file
    #
    try:
        fpInputInt = open(inputFileInt, 'w')
    except:
        print('Cannot open file: ' + inputFileInt)
        return 1

    #
    # Open the Log Diag file.
    #
    try:
        fpLogDiag = open(logDiagFile, 'a+')
    except:
        print('Cannot open file: ' + logDiagFile)
        return 1

    #
    # Open the Log Cur file.
    #
    try:
        fpLogCur = open(logCurFile, 'a+')
        fpLogCur.write('\n\n######################################\n')
        fpLogCur.write('########## Preprocess Log ##############\n')
        fpLogCur.write('######################################\n\n')

    except:
        print('Cannot open file: ' + logCurFile)
        return 1

    return 0

#
# Purpose: Close files.
# Returns: 0
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles():

    if fpInputInt:
        fpInputInt.close()


    if fpLogCur:
        fpLogCur.close()

    if fpLogDiag:
        fpLogDiag.close()
    return 0

#
# Purpose: parse the set of input files
# Returns: 0
# Assumes: 
# Effects: Nothing
# Throws: Nothing
#
def parseInputFiles():
    # loop through the configured files parsing out pertinent info to the intermediate file
    
    # parse out the columns we want:
    # MP ID = subject_id
    # HP ID = object_id
    # predicate value = predicate_id (load only those in predicateIncludeList)
    # mapping justification = mapping_justification

    # used for checking dupes in the input
    lineList = []

    for file in str.split(os.getenv('INPUT_FILE_NAMES')):
        print('file: %s' % file)

        # The list of column headers for the current file, columns are ordered differently
        # in different files, but have same text
        headers = []

        fpInput = open('%s/%s' % (downloadDir, file))
        fpLogCur.write('%sFile: %s%s' % (CRT, file, CRT))

        # number of actual records in this file, including dupes
        recordCt = 0

        # number of records skipped because duplicate
        dupeCt = 0

        # number of records written to the intermediate file
        goodCt = 0

        # number of records skipped because predicate not in list
        badPredCt = 0

        # number of records skipped because MP is blank
        blankMpCt = 0

        # number of records skipped because HP is blank
        blankHpCt = 0

        # MP ID is not in the database or is not a preferred ID
        badMpCt = 0

        # HP  ID is not in the database or is not a preferred ID
        badHpCt = 0

        # No HP ID in the input record
        hpNotFoundCt = 0

        for line in fpInput.readlines():
            line = str.strip(line) # remove CRT at end of line
            #print('line: %s' % line)
            if str.find(line, '#') == 0:
                continue
            elif str.find(line, 'subject_id') != -1:
                headers = str.split(line, TAB)
                #print('headers: %s' % headers)
                continue
            recordCt +=1

            tokens = str.split(line, TAB)
            # parse out the columns we want
            # MP ID = subject_id      
            # HP ID = object_id
            # predicate value = predicate_id (load only those in predicateIncludeList)
            # mapping justification = mapping_justification       
            mpID = tokens[headers.index('subject_id')]
            hpID = tokens[headers.index('object_id')]

            if mpID == '':
                fpLogCur.write('MP ID is blank: %s' % (line, CRT))
                blankMpCt += 1
                continue

            if hpID == '':
                fpLogCur.write('HP ID is blank: %s' % (line, CRT))
                blankHpCt += 1
                continue

            if mpID not in mpDict:
                badMpCt += 1
                fpLogCur.write('Invalid MP ID: %s%s' % (line, CRT))
                continue

            if hpID not in hpDict:
                if hpID == 'sssom:NoTermFound':
                    fpLogCur.write('HP ID sssom:NoTermFound: %s%s' % (line, CRT))
                    hpNotFoundCt += 1
                else:
                    fpLogCur.write('Invalid HP ID: %s%s' % (line, CRT))
                    badHpCt += 1
                continue

            predicate = tokens[headers.index('predicate_id')]
            if predicate == '':
                predicate = unspecified
            if predicate not in predicateIncludeList:
                badPredCt += 1
                fpLogCur.write('Non-configured predicate: %s%s' % (line, CRT))
                continue

            # strip off the prefix if it exists
            predicate = predicate.split(':')[1]
            
            mapjust = tokens[headers.index('mapping_justification')]
            if mapjust == '':
                mapjust = unspecified

            # strip off the prefix if it exists
            mapjust = mapjust.split(':')[1]

            # At this point we know the mp and hp IDs are valid and in the dict
            # Get the key and write to intermediate file - saves us this step
            # in the processor scrip
            mpKey = mpDict[mpID]
            hpKey = hpDict[hpID]

            lineToWrite = '%s%s%s%s%s%s%s%s%s%s%s%si%s%s' % (mpID, TAB, mpKey, TAB, hpID, TAB, hpKey, TAB, predicate, TAB, mapjust, TAB, file, CRT)
            # skip any duplicates
            if lineToWrite in lineList:
                #print('Dupe Line: %s' % lineToWrite)
                dupeCt +=1
                fpLogCur.write('Dupe Line: %s' % lineToWrite)
                continue
            lineList.append(lineToWrite)
            fpInputInt.write(lineToWrite)
            goodCt += 1    
        fpInput.close()
        fpLogCur.write('Total Records: %s%s' % (recordCt, CRT))
        fpLogCur.write('Total Dupes: %s%s' % (dupeCt, CRT))
        fpLogCur.write('Total Records with Blank MP ID: %s%s' % (blankMpCt, CRT))
        fpLogCur.write('Total Records with Blank HP ID: %s%s' % (blankHpCt, CRT))

        fpLogCur.write('Total Records with Invalid MP ID: %s%s' % (badMpCt, CRT))
        fpLogCur.write('Total Records with Invalid HP ID: %s%s' % (badHpCt, CRT))
        fpLogCur.write('Total Records with HP sssom:NoTermFound: %s%s' % (hpNotFoundCt, CRT))
        fpLogCur.write('Total Records with non-configured Predicate: %s%s' % (badPredCt, CRT))
        fpLogCur.write('Total Records written to Intermediate File: %s%s' % (goodCt, CRT))
        # -- end of current file parsing
    # -- end of parsing files


    return 0

#
#  MAIN
#

print('initialize: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if initialize() != 0:
    sys.exit(1)

print('openFiles: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if openFiles() != 0:
    sys.exit(1)

print('parseInputFiles: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if parseInputFiles() != 0:
    sys.exit(1)

closeFiles()
print('done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.exit(0)
