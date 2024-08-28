from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
import datetime
from datetime import datetime, timedelta, date
from System import Array, String, DateTime
from Spotfire.Dxp.Data import DataValueCursor, IndexSet, AddRowsSettings, RowSelection
from Spotfire.Dxp.Application.Visuals import HtmlTextArea

ps = Application.GetService[ProgressService]()
sql = u"select count(YEAR_ID) FROM DC_E_BULK_CM_MANAGEDELEMENT_RAW"
dataSourceName = Document.Properties["DataSourceName"]
databaseConnectionResult = "DatabaseConnectionResultAdmin"
dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
tableName = 'Test DB Connection'
netAnConnection = Document.Properties["NetAnResponseCode"]
print "dataSourceName",dataSourceName
def fetchDataFromENIQAsync():
    Document.Properties["SelectedDate"]="01/01/1999"
    if netAnConnection == 'Connection OK':
        if dataSourceName != '':
            try:
                ps.CurrentProgress.ExecuteSubtask('Testing Connection to %s ...' % (dataSourceName))
                ps.CurrentProgress.CheckCancel()
                dataTableDataSource = DatabaseDataSource(dataSourceSettings)
                dt = Document.Data.Tables.Add(tableName, dataTableDataSource)
                if Document.Data.Tables.Contains(tableName):      # If exists, remove it
                    Document.Data.Tables.Remove(dt)
                Document.Properties[databaseConnectionResult] = 'Connection OK'
                Document.Properties["RepDBSourceName"]=Document.Properties["DataSourceName"]+"repdb"
                Document.Properties['TriggerFetchAttribute']=DateTime.UtcNow
                vis.As[HtmlTextArea]().HtmlContent += " "
            except: 
                Document.Properties["RepDBSourceName"]=""
                Document.Properties[databaseConnectionResult] = 'Connection Failed'
                refreshdates = Document.Data.Tables['Available Dates']
                refreshdates.RemoveRows(RowSelection(IndexSet(refreshdates.RowCount,True)))
                fetchattributes = Document.Data.Tables['CM Attributes']
                fetchattributes.RemoveRows(RowSelection(IndexSet(fetchattributes.RowCount,True)))
        else:
            Document.Properties[databaseConnectionResult] = 'Enter Data Source Name'
    else:
        Document.Properties[databaseConnectionResult] = 'Connect to NetAn DB before connection ENIQ'

ps.ExecuteWithProgress('Testing Connection to %s ...' % (dataSourceName), 'Testing Connection to %s ...' % (dataSourceName), fetchDataFromENIQAsync)
