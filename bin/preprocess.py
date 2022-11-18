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

errorDisplay = '''

***********
errMsg: %s
%s
'''

downloadDir = os.getenv('DOWNLOAD_DIR')
predicateIncludeList = str.split(os.getenv('PREDICATES_TO_LOAD'), ', ')

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

    inputFileInt = os.getenv('INPUT_FILE_TOLOAD')
    logDiagFile = os.getenv('LOG_DIAG')
    logCurFile = os.getenv('LOG_CUR')

    db.useOneConnection(1)

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
# Purpose: Log a message to the diagnostic log, optionally
#	write a line to the error file. Write to error Dict
#	which is used to sort errors and will be written to 
#	curation log later
# Returns: 0
# Assumes: file descriptors exist
# Effects: Nothing
# Throws: Nothing
#
def logIt(msg, line, isError, typeError):
    global errorDict
    logit = errorDisplay % (msg, line)
    fpLogDiag.write(logit)
    if not typeError in errorDict:
        errorDict[typeError] = []
    errorDict[typeError].append(logit)
    if isError:
        fpHTMPError.write(line)

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

        for line in fpInput.readlines():
            line = str.strip(line) # remove CRT at end of line
            #print('line: %s' % line)
            if str.find(line, '#') == 0:
                continue
            elif str.find(line, 'subject_id') != -1:
                headers = str.split(line, TAB)
                print('headers: %s' % headers)
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
            predicate = tokens[headers.index('predicate_id')]
            if predicate not in predicateIncludeList:
                badPredCt += 1
                fpLogCur.write('Non-configured predicate: %s%s' % (line, CRT))
                continue
            mapjust = tokens[headers.index('mapping_justification')]
            lineToWrite = '%s%s%s%s%s%s%s%si%s%s' % (mpID, TAB, hpID, TAB, predicate, TAB, mapjust, TAB, file, CRT)
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
