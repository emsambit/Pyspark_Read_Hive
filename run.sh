## Variables included are -  PATH TO HIVE-SITE.XML                                                #
#Description:
#The script used to read HIVE-SITE.XML file from cluster and pass necessary values to the
#calling program
#
#The RETURN values are used by SPARK  to connect HIVE database
##================================================================================================#
## REVISION HISTORY                                                                               #
##------------------------------------------------------------------------------------------------#
## 2019-09-26:: v1.0  :: Sambit Baliarsingh     :: Created                                        #
##================================================================================================#

export SPARK_MAJOR_VERSION=2

echo "entering spark"

LogMessage()
{
  echo [`date +%d-%b-%Y" "%H:%M`]    $1  >> ${REPL_LOG}
}
SendMail()
{
  echo "The script repl_sku_invt_dly.py has failed.\n\nReason:\n${1}.\n\nAction:\nPlease check log ${REPL_LOG} and take appropriate action." | mailx -s "repl_sku_invt_dly.py failed !"  "${TO_MAIL_LIST}"
}
Send_success_Mail()
{
echo "Script repl_sku_invt_dly.py completed successfully.\n\n.Please check log ${REPL_LOG}." | mailx -s "repl_sku_invt_dly.py succeeded !"  "${TO_MAIL_LIST}"
}
export CFG_DIR=`dirname $0`
export CFGFILE=config.cfg
if [ -f ${CFG_DIR}/conf/${CFGFILE} ];
  then
. ${CFG_DIR}/conf/${CFGFILE}
        #### Remove Old Log File
        rm ${LOG_DIRECTORY}/*.log
        LogMessage "----------------------------------------Execution started----------------------------------------------------\n"
        echo "Process started " `date +%d-%b-%Y" "%H:%M`
        LogMessage "1.Config File Invoked"
else
        echo "ERROR: The Configuration file ${CFG_DIR}/${CFGFILE} does not exist ....."
        echo "Exiting....."
	SendMail "Script sc_dc  failed while invoking the cfg file"
        exit 1
fi

hive_setting=$(python2.7 ${PY_FILE_PATH}/read_hive_xml.py ${HIVE_XML_PATH}/hive-site.xml)

hive_param="${hive_setting}${SPARK_SETTING}"
target_table='tablename'

echo "$hive_param"
LogMessage "hive parameters are belwo"
LogMessage $hive_param

LogMessage "------------------------------------ SPARK SUBMIT STARTED--------------------------------------------"

#pyfile=${py_file_path}"/""sc_dc_inv_dly_load.py"
#echo ${pyfile}

spark-submit --master yarn --deploy-mode cluster --queue mdsedatload ${PY_FILE_PATH}/spark_program.py ${hive_param} ${target_table}

LogMessage "------------------------------------ SPARK EXECUTION FINISHED--------------------------------------------"
