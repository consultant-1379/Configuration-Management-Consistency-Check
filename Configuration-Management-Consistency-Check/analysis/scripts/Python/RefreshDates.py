from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *

sql = u"select distinct(DATE_ID) FROM DC_E_BULK_CM_BULKCMCONFIGDATAFILE_RAW order by DATE_ID desc"
dataSourceName = Document.Properties["DataSourceName"]
databaseConnectionResult = "DatabaseConnectionResult"
dataSourceSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
tableName = 'Available Dates'


def fetchDataFromENIQAsync():
    try:
        dataTableDataSource = DatabaseDataSource(dataSourceSettings)
        if Document.Data.Tables.Contains(tableName):      # If exists, replace it
            Document.Data.Tables[tableName].ReplaceData(dataTableDataSource)
        else:                                             # If it does not exist, create new
            Document.Data.Tables.Add(tableName, dataTableDataSource)
        Document.Properties[databaseConnectionResult] = 'Connection OK'
    except:
        Document.Properties[databaseConnectionResult] = 'Connection Failed'



fetchDataFromENIQAsync()
# Fix sort order so that newest dates are first in the drop down list
values = Document.Data.Tables[tableName].Columns['DATE_ID'].RowValues.GetEnumerator()
myValues = []
for val in values:
    myValues.Add(val.ValidValue)
myValues.sort(reverse=True)
Document.Data.Tables[tableName].Columns['DATE_ID'].Properties.SetCustomSortOrder(myValues)
