#!/bin/sh

#
# This script is a wrapper around the process that loads 
# MP/HP mappings
#
#
#     mp_hpmappingload.sh 
#

cd `dirname $0`/..
CONFIG_LOAD=`pwd`/mp_hpmappingload.config

cd `dirname $0`
LOG=`pwd`/mp_hpmappingload.log
rm -rf ${LOG}

USAGE='Usage: mp_hpmappingload.sh'

#
#  Verify the argument(s) to the shell script.
#
if [ $# -ne 0 ]
then
    echo ${USAGE} | tee -a ${LOG}
    exit 1
fi

#
# verify & source the configuration file
#

if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}"
    exit 1
fi

. ${CONFIG_LOAD}

#
# Just a verification of where we are at
#

echo "MGD_DBSERVER: ${MGD_DBSERVER}"
echo "MGD_DBNAME: ${MGD_DBNAME}"

#
#  Source the DLA library functions.
#

if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#
# loop over in INPUT_FILE_NAMES in DOWNLOAD_DIR
# checking that they exist and copying them to INPUTDIR 
# if any are missing, abort the load
#

for file in ${INPUT_FILE_NAMES}
do
    echo "${DOWNLOAD_DIR}/${file}"
    if [ -f ${DOWNLOAD_DIR}/${file} ]
    then
        cp ${DOWNLOAD_DIR}/${file} ${INPUTDIR}
    else
        STAT=1
        checkStatus ${STAT} "Missing file aborting the load: ${DOWNLOAD_DIR}/${file}"
    fi
done

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#

preload ${OUTPUTDIR}

#
# rm all files/dirs from OUTPUTDIR
#

cleanDir ${OUTPUTDIR}

echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run Preprocessor"  | tee -a ${LOG_DIAG}
${MPHPMAPPINGLOAD}/bin/preprocess.py 
STAT=$?
if [ ${STAT} -eq 1 ]
then
    checkStatus ${STAT} "An error occurred while running the preprocessor See ${LOG_DIAG}. preprocessMP_HP.sh"

    # run postload cleanup and email logs
    shutDown
fi

#
# run the load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run process.py"  | tee -a ${LOG_DIAG}
${PYTHON} ${MPHPMAPPINGLOAD}/bin/process.py
STAT=$?
checkStatus ${STAT} "${MPHPMAPPINGLOAD}/bin/process.py"

# run postload cleanup and email logs

shutDown

