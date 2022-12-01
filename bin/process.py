#
#  process.py
###########################################################################
#
#  Purpose:
#
#      Create MM/HP Relationships from the intermediate file
#
#  Usage:
#
#      process.py 
#
#  Inputs:
#
#	1. load-ready MP/HP file tab-delimited in the following format
#	    1. MP ID
#           2. MP Term Key 
#	    3. HP ID
#	    4. HPO Term Key
#           5. Predicate
#           6. Justification
#           7. Filename
#
#	2. Configuration - see mp_hpoload.config
#
#  Outputs:
#
#       1. MGI_Relationship.bcp
#       2. MGI_Relationship_Property.bcp
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  bcp fails

#  Assumes:
#
#      1) Preprocessor has done all QC and resolved IDs to keys
#	
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Parse the input file and create bcp files 
#      5) Close the input/output files.
#      6) Delete existing relationships
#      7) BCP in new relationships:
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
#  11/21/2022  sc  Initial development
#
###########################################################################

import sys
import os
import time

import db
import mgi_utils

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'
DATE = mgi_utils.date("%m/%d/%Y")
USAGE='process.py'

#
#  GLOBALS
#

# input file
inFile = os.environ['INPUT_FILE_TOLOAD']

# output bcp files
relBcpFile =   os.environ['RELATIONSHIP_BCP']
propBcpFile = os.environ['PROPERTY_BCP']

outputDir = os.environ['OUTPUTDIR']
relationshipFile = '%s/%s' % (outputDir, relBcpFile)
propertyFile = '%s/%s' % (outputDir, propBcpFile)

# if 'true',bcp files will not be bcp-ed into the database.
# Default is 'false'
DEBUG = os.getenv('LOG_DEBUG')

# file descriptors
fpInFile = ''
fpRelationshipFile = ''
fpPropertyFile = ''

# The mp hp mapping relationship category key 'mp_to_hpo'
catKey = 1011

# the mp hp mapping relationship term key 'mp_to_hpo'
relKey = 109626615

# the mp hp mapping qualifier key 'Not Specified'
qualKey = 11391898

# the mp hp mapping evidence key 'Not Specified'
evidKey = 17396909

# the mp hp mapping reference key J:331145
refsKey = 596149

# mp hp mapping load user key
userKey = 1635

# predicate property name key
predPropNameKey = 109733907 # mapping_predicate
predSeqNum = 1

# justification property name key
justPropNameKey = 109733906 # mapping_justification
justSeqNum = 2

# filename property name key
filePropNameKey =  11588492 # data_source
fileSeqNum = 3

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key
nextPropertyKey = 1000          # MGI_Relationship_Property._RelationshipProperty_key

# for bcp
bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
relTable = 'MGI_Relationship'
propTable = 'MGI_Relationship_Property'

def initialize():
    # Purpose: create lookups, open files, create db connection, gets max
    #	keys from the db
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, creates files in the file system, creates connection to a database

    global nextRelationshipKey, nextPropertyKey

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    user = os.environ['MGD_DBUSER']
    passwordFileName = os.environ['MGD_DBPASSWORDFILE']
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)

    #
    # get next MGI_Relationship key
    #
    results = db.sql('''select nextval('mgi_relationship_seq') as nextKey''', 'auto')
    if results[0]['nextKey'] is None:
        nextRelationshipKey = 1000
    else:
        nextRelationshipKey = results[0]['nextKey']


    #
    # get next MGI_Relationship_Property key
    #
    results = db.sql('''select nextval('mgi_relationship_property_seq') as nextKey''', 'auto')
    if results[0]['nextKey'] is None:
        nextPropertyKey = 1000
    else:
        nextPropertyKey = results[0]['nextKey']

    return 0

# end initialize() -------------------------------

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables
    #  creates files in the file system

    global fpInFile, fpRelationshipFile, fpPropertyFile

    try:
        fpInFile = open(inFile, 'r')
    except:
        print('Cannot open Feature relationships input file: %s' % inFile)
        return 1

    try:
        fpRelationshipFile = open(relationshipFile, 'w')
    except:
        print('Cannot open Feature relationships bcp file: %s' % relationshipFile)
        return 1

    try:
        fpPropertyFile = open(propertyFile, 'w')
    except:
        print('Cannot open Feature properties bcp file: %s' % propertyFile)
        return 1

    return 0

# end openFiles() -------------------------------


def closeFiles ():
    # Purpose: Close all file descriptors
    # Returns: Nothing
    # Assumes: all file descriptors were initialized
    # Effects: Nothing
    # Throws: Nothing

    global fpInFile, fpRelationshipFile, fpPropertyFile

    try:
        fpInFile.close()
        fpRelationshipFile.close()
        fpPropertyFile.close()
    except:
        return 1

    return 0

# end closeFiles() -------------------------------

def process( ): 
    # Purpose: parses intermediate MP/HP Mapping file and creates bcp files
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: sets global variables, writes to the file system
    # Throws: Nothing

    global nextRelationshipKey, nextPropertyKey

    #
    # Iterate through the load ready input file
    #
    for line in fpInFile.readlines():
        tokens = list(map(str.strip, str.split(line, TAB)))
        mpId = tokens[0]
        objKey1 = tokens[1]
        hpId = tokens[2]
        objKey2 =  tokens[3]
        predicate = tokens[4]
        justification = tokens[5]
        fileName = tokens[6]
        
        # MGI_Relationship
        fpRelationshipFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
            (nextRelationshipKey, TAB, catKey, TAB, objKey1, TAB, objKey2, TAB, relKey, TAB, qualKey, TAB, evidKey, TAB, refsKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

        # MGI_Relationship_Property predicate
        fpPropertyFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextPropertyKey, TAB, nextRelationshipKey, TAB, predPropNameKey, TAB, predicate, TAB, predSeqNum, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT ) )

        nextPropertyKey += 1

        # MGI_Relationship_Property justification
        fpPropertyFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextPropertyKey, TAB, nextRelationshipKey, TAB, justPropNameKey, TAB, justification, TAB, justSeqNum, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT ) )

        nextPropertyKey += 1

        # MGI_Relationship_Property predicate filename
        fpPropertyFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % (nextPropertyKey, TAB, nextRelationshipKey, TAB, filePropNameKey, TAB, fileName, TAB, fileSeqNum, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT ) )

        nextPropertyKey += 1

        nextRelationshipKey += 1

    return 0

# end process() -------------------------------------

def doDeletes():
    # cascades to MGI_Relationship_Property
    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()

    return 0

# end doDeletes() -------------------------------------

def bcpFiles():
    if DEBUG  == 'true':
        return 0

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, relTable, outputDir, relBcpFile)
    rc = os.system(bcpCmd)

    # update mgi_relationship_seq auto-sequence
    db.sql(''' select setval('mgi_relationship_seq', (select max(_Relationship_key) from MGI_Relationship)) ''', None)
    db.commit()

    if rc != 0:
        closeFiles()
        print('Error bcping relationship file')
        return 1 

    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, propTable, outputDir, propBcpFile)
    rc = os.system(bcpCmd)

    # update mgi_relationship_property_seq auto-sequence
    db.sql(''' select setval('mgi_relationship_property_seq', (select max(_RelationshipProperty_key) from MGI_Relationship_Property)) ''', None)
    db.commit()

    db.useOneConnection(0)

    if rc != 0:
        closeFiles()
        print('Error bcping property file')
        return 1
    return 0

#####################
#
# Main
#
#####################

print('initialize: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if initialize() != 0:
    exit(1, 'Error in  initialize \n' )

print('process: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if process() != 0:
    print('Error in the process method')
    sys.exit(1)

print('closeFiles: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if closeFiles() != 0:
    print('Error closing files')
    sys.exit(1)

print('doDeletes: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if doDeletes() != 0:
    print('Error doing deletes')
    sys.exit(1)

print('bcpFiles: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
if bcpFiles()  != 0:
    print('Error executing bcp')
    sys.exit(1)

sys.exit(0)
