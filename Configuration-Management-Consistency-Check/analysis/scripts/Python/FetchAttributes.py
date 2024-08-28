# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2023 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : FetchAttribute.py
# Date    : 16/10/2023
# Revision: 1.0
# Purpose : Fetches ENIQ tables need for the attribute table
#
# Usage   : CMCC Analysis
#

from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
from Spotfire.Dxp.Data import DataValueCursor

notify = Application.GetService[NotificationService]()

from datetime import *

def fetchDataFromENIQAsync(tableName,sql):
    try:
        dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        if Document.Data.Tables.Contains(tableName):      # If exists, replace it
            Document.Data.Tables[tableName].ReplaceData(dataTableDataSource)
        else:       # If it does not exist, create new
            Document.Data.Tables.Add(tableName, dataTableDataSource)
        Document.Properties[databaseConnectionResult] = 'Connection OK'
    except:
        Document.Properties[databaseConnectionResult] = 'Connection Failed'

def createDataTables():
    try:
        SQL = " select MC.TABLENAME as TableName,MC.DATANAME as Attribute,MC.DATATYPE as ENIQDataType,DT.TAGID as MOClass from (SELECT TYPEID,UPPER(RIGHT(TYPEID, CHARINDEX(':', REVERSE(TYPEID)) - 1)) AS TABLENAME, DATANAME, DATATYPE FROM MeasurementCounter MC INNER JOIN TPACTIVATION TP ON MC.TYPEID LIKE TP.VERSIONID+'%' WHERE TYPEID LIKE '%DC_E_BULK_CM%') MC left outer join ( select if(SUBSTRING(TAGID,CHARINDEX('_',TAGID),LEN(TAGID)-1) in ('_V')) THEN SUBSTRING(TAGID,0,CHARINDEX('_',TAGID)-1) ELSE TAGID ENDIF as TAGID,substring(DT.DATAFORMATID,0,length(DT.DATAFORMATID)-CHARINDEX(':', REVERSE(DT.DATAFORMATID))) AS TABLENAME_temp,UPPER(RIGHT(TABLENAME_temp, CHARINDEX(':', REVERSE(TABLENAME_temp)) - 1)) AS TABLENAME from DefaultTags DT  INNER JOIN TPACTIVATION TP ON DT.DATAFORMATID LIKE TP.VERSIONID+'%' WHERE DATAFORMATID LIKE '%DC_E_BULK_CM%') DT on MC.TABLENAME= DT.TABLENAME where MC.TableName like 'DC_E_BULK_CM_AIDEVICE' or MC.TableName like 'DC_E_BULK_CM_AIDEVICE_V' or MC.TableName like 'DC_E_BULK_CM_BEAM' or MC.TableName like 'DC_E_BULK_CM_COMMONBEAMFORMING' or MC.TableName like 'DC_E_BULK_CM_EUTRANFREQRELATION' or  MC.TableName like 'DC_E_BULK_CM_EUTRAFREQRELATION'  "
        fetchDataFromENIQAsync("CM Attributes",SQL)
        
    except Exception as e:
            print("Exception: ", str(e))


def main():
    try:
        createDataTables()

    except Exception as e:
        print("Exception: ", str(e))
        notify.AddWarningNotification("Exception","Error in fetching CM Attributes", e)
   

data_manager = Document.Data
app = Application
data_function = None
dataSourceName = Document.Properties["RepDBSourceName"]
databaseConnectionResult = "DatabaseConnectionResult"
main()

Document.Properties['TriggerFetchAvailableDates'] = str(datetime.utcnow())

