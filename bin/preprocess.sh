#!/bin/sh
#
#  preprocess.sh
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the process that parses the set of
#	mp/hp files from different providers, does QC and puts the required info 
#       in an intermediate file
#	***It is a convenience script for development.***
#
Usage="Usage: preprocess.sh" 
#
#  Env Vars:
#
#      See the configuration file 
#
#  Inputs:  None
#
#  Outputs:
#
#      - Log file (${LOG_DIAG})
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
#      1) Source the configuration file to establish the environment.
#      2) Verify that the input file exists.
#      3) Establish the log file.
#      4) Call preprocess.py to create the intermediate file
#
#  Notes:  
#
#  11/17/2022   sc
#       - TR11893
#
###########################################################################
cd `dirname $0`/..
CONFIG_LOAD=`pwd`/mp_hpmappingload.config
echo "CONFIG_LOAD: ${CONFIG_LOAD}"
echo `dirname $0`
cd `dirname $0`
#
# Make sure the configuration file exists and source it.
#
if [ -f ${CONFIG_LOAD} ]
then
    . ${CONFIG_LOAD}
else
    echo "Missing configuration file: ${CONFIG_LOAD}"
    exit 1
fi

#
# Establish the diagnostic log file.
#
LOG=${LOG_DIAG}
rm ${LOG}
echo "LOG: ${LOG}"
touch ${LOG}

rm  ${LOG_CUR}
echo "LOG_CUR: ${LOG_CUR}"
touch ${LOG_CUR}

#
# Create the intermediate input files
#
echo "" >> ${LOG}
date >> ${LOG}
echo 'calling preprocess.py'
${PYTHON} ${MPHPMAPPINGLOAD}/bin/preprocess.py #2>&1 >> ${LOG}
STAT=$?
if [ ${STAT} -eq 1 ]
then
    echo "Error: creating the MP/HP intermediate file (preprocess.py)" | tee -a ${LOG}
    exit 1
fi
date >> ${LOG}
