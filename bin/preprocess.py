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
#   2. MP term label
#   3. HP ID - relationship participant
#   4. HP term label 
#   5. Predicate ID - property
#   6. Mapping Justification - property
#   7. Input file name - property
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
# {mpID:[key, term], ...}
# preferred
mpDict = {}

# non-preferred
mpNpDict = {}

# {hpID:[key, term], ...}
# preferred
hpDict = {}

# non-preferred
hpNpDict = {}

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
    global mpDict, mpNpDict, hpDict, hpNpDict

    inputFileInt = os.getenv('INPUT_FILE_TOLOAD')
    logDiagFile = os.getenv('LOG_DIAG')
    logCurFile = os.getenv('LOG_CUR')

    db.useOneConnection(1)

    # lookup of preferred MP IDs/terms
    results = db.sql('''select a.accid, a._object_key, t.term
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 34
        and a.preferred = 1
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        mpDict[r['accid']] = [r['_object_key'], r['term']]

    # lookup of non-preferred MP IDs/terms
    results = db.sql('''select a.accid, a._object_key, t.term
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 34
        and a.preferred = 0
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        mpNpDict[r['accid']] = [r['_object_key'], r['term']]

    # lookup of preferred HP IDs/terms
    results = db.sql('''select a.accid, a._object_key, t.term
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 180
        --and a.preferred = 1
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        hpDict[r['accid']] = [r['_object_key'], r['term']]

    # lookup of non-preferred HP IDs/terms
    results = db.sql('''select a.accid, a._object_key, t.term
        from acc_accession a, voc_term t
        where a._mgitype_key = 13
        and a._logicaldb_key = 180
        --and a.preferred = 0
        and a._object_key = t._term_key''', 'auto')
    for r in results:
        hpNpDict[r['accid']] = [r['_object_key'], r['term']]

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
        fpLogCur.write('\n#############################################\n')
        fpLogCur.write('############# Preprocess Log ################\n')
        fpLogCur.write('## Ordered by Line Number within each file ##\n')
        fpLogCur.write('#############################################\n\n')

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

    totalGoodCt = 0 
    for fileName in str.split(os.getenv('INPUT_FILE_NAMES')):
        print('fileName: %s' % fileName)

        # The list of column headers for the current file, columns are ordered differently
        # in different files, but have same text
        headers = []

        fpInput = open('%s/%s' % (downloadDir, fileName))
        fpLogCur.write('%sFile: %s%s' % (CRT, fileName, CRT))

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

        # MP ID is not in the database 
        badMpCt = 0

        # HP  ID is not in the database 
        badHpCt = 0
      
        # Input MP term does not match database term 
        mpBadTermCt = 0

        # Input HP term does not match database term
        hpBadTermCt = 0

        # MP ID is not a preferred ID
        npMpCt = 0

        # HP  ID is not a preferred ID
        npHpCt = 0

        # No HP ID in the input record
        hpNotFoundCt = 0

        # current line number in current file
        lineNum = 0
        for line in fpInput.readlines():
            lineNum += 1
            # set to false if a non-preferred ID, which we will report and load.
            mpPreferred = 1
            hpPreferred = 1
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
            mpTermLabel =  tokens[headers.index('subject_label')]
            hpID = tokens[headers.index('object_id')]
            hpTermLabel =  tokens[headers.index('object_label')]

            if mpID == '':
                fpLogCur.write('Line %s - MP ID is blank: %s' % (lineNum, line, CRT))
                blankMpCt += 1
                continue

            if hpID == '':
                fpLogCur.write('Line %s - HP ID is blank: %s' % (lineNum, line, CRT))
                blankHpCt += 1
                continue

            if mpID not in mpDict:
                if mpID not in mpNpDict:
                    badMpCt += 1
                    fpLogCur.write('Line %s - Invalid MP ID: %s%s' % (lineNum, line, CRT))
                    continue
                else:
                    mpPreferred = 0
                    fpLogCur.write('Line %s - Non-preferred MP ID (relationship loaded): %s%s' % (lineNum, line, CRT))
                    npMpCt +=1                   

            if hpID not in hpDict:
                if hpID == 'sssom:NoTermFound':
                    fpLogCur.write('Line %s - HP ID sssom:NoTermFound: %s%s' % (lineNum, line, CRT))
                    hpNotFoundCt += 1
                    continue
                elif hpID not in hpNpDict:
                    fpLogCur.write('Line %s - Invalid HP ID: %s%s' % (lineNum, line, CRT))
                    badHpCt += 1
                    continue
                else:
                    hpPreferred = 0
                    fpLogCur.write('Line %s - Non-preferred HP ID (relationship loaded): %s%s' % (lineNum, line, CRT))       
                    npHpCt += 1

            predicate = tokens[headers.index('predicate_id')]
            if predicate == '':
                predicate = unspecified
            if predicate not in predicateIncludeList:
                badPredCt += 1
                fpLogCur.write('Line %s - Non-configured predicate: %s%s' % (lineNum, line, CRT))
                continue

            # strip off the prefix if it exists
            predicate = predicate.split(':')[1]
            
            mapjust = tokens[headers.index('mapping_justification')]
            if mapjust == '':
                mapjust = unspecified

            # strip off the prefix if it exists
            mapjust = mapjust.split(':')[1]

            # At this point we know the mp and hp IDs are valid (preferred or not)
            # Get the key and term, write to intermediate file - saves us this step
            # in the processor script. QC the term against the database
            if mpPreferred:
                mpKey = mpDict[mpID][0]
                mpDbTerm = mpDict[mpID][1]
            else:
                mpKey = mpNpDict[mpID][0]
                mpDbTerm = mpNpDict[mpID][1]

            if hpPreferred:
                hpKey = hpDict[hpID][0]
                hpDbTerm = hpDict[hpID][1]
            else:
                hpKey = hpNpDict[hpID][0]
                hpDbTerm = hpNpDict[hpID][1]

            # Don't use mpTermLabel or hpTermLabel when looking for dupes, there could be dupes
            # that have different mp/hp term labels 
            lineForDupeCheck = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (mpID, TAB, mpKey, TAB, hpID, TAB, hpKey, TAB, predicate, TAB, mapjust, TAB, fileName, CRT)

            lineToWrite = '%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (mpID, TAB, mpTermLabel, TAB, mpKey, TAB, hpID, TAB, hpTermLabel, TAB, hpKey, TAB, predicate, TAB, mapjust, TAB, fileName, CRT)

            # skip any duplicates
            if lineForDupeCheck in lineList:
                #print('Dupe Line: %s -  %s' % (lineNum, lineToWrite))
                dupeCt +=1
                fpLogCur.write('Dupe Line: %s - %s' % (lineNum, lineToWrite))
                continue

            # Now AFTER we check for dupes, report discrepancies between term labels and database terms
            if str.lower(mpDbTerm) != str.lower(mpTermLabel):
                fpLogCur.write('Line %s - Database MP Term: "%s" does not match input term(relationship loaded): %s%s' % (lineNum, mpDbTerm, line, CRT))
                mpBadTermCt += 1

            if str.lower(hpDbTerm) != str.lower(hpTermLabel):
                fpLogCur.write('Line %s - Database HP Term: "%s" does not match input term(relationship loaded): %s%s' % (lineNum, hpDbTerm, line, CRT))
                hpBadTermCt += 1

            lineList.append(lineForDupeCheck)
            fpInputInt.write(lineToWrite)
            goodCt += 1    
            totalGoodCt += 1
        fpInput.close()
        fpLogCur.write('Total Records: %s%s' % (recordCt, CRT))
        fpLogCur.write('Total Dupes: %s%s' % (dupeCt, CRT))
        fpLogCur.write('Total Records with Blank MP ID: %s%s' % (blankMpCt, CRT))
        fpLogCur.write('Total Records with Blank HP ID: %s%s' % (blankHpCt, CRT))

        fpLogCur.write('Total Records with Invalid MP ID: %s%s' % (badMpCt, CRT))
        fpLogCur.write('Total Records with Invalid HP ID: %s%s' % (badHpCt, CRT))
        
        fpLogCur.write('Total Records with Secondary MP ID (relationship loaded): %s%s' % (npMpCt, CRT))
        fpLogCur.write('Total Records with Secondary HP ID (relationship loaded): %s%s' % (npHpCt, CRT))

        fpLogCur.write('Total Records where input MP label does not match database term (relationship loaded): %s%s' % (mpBadTermCt, CRT))
        fpLogCur.write('Total Records where input HP label does not match database term (relationship loaded): %s%s' % (hpBadTermCt, CRT))

        fpLogCur.write('Total Records with HP sssom:NoTermFound: %s%s' % (hpNotFoundCt, CRT))
        fpLogCur.write('Total Records with non-configured Predicate: %s%s' % (badPredCt, CRT))
        fpLogCur.write('Total Records written to Intermediate File: %s%s' % (goodCt, CRT))
        # -- end of current file parsing

    fpLogCur.write('%sTotal Records from all files written to Intermediate File: %s%s' % (CRT, totalGoodCt, CRT))

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
