from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Application import PanelTypeIdentifiers
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters
from System import Array, String, DateTime

def create_cursor(eTable):
    """Returns cursor for a data table."""
    cursList = []
    colList = []
    colname = []
    for eColumn in eTable.Columns:
        cursList.append(DataValueCursor.CreateFormatted(eTable.Columns[eColumn.Name]))
        colList.append(eTable.Columns[eColumn.Name].ToString())
    cursArray = Array[DataValueCursor](cursList)
    cusrDict = dict(zip(colList, cursList))
    return cusrDict
    
    
def get_selected_values():
    moclass_set=set()
    attribute_list_data_table = Document.Data.Tables['CM Attributes']
    data_filtering_selection = Document.Data.Filterings['Filtering scheme (MOClass)']
    filtering_scheme = Document.FilteringSchemes[data_filtering_selection]
    filter_collection = filtering_scheme[attribute_list_data_table]
    filtered_rows = filter_collection.FilteredRows
    my_col_cursor = DataValueCursor.CreateFormatted(attribute_list_data_table.Columns["MOClass"])
    rowCount = 0
    for row in attribute_list_data_table.GetRows(filtered_rows,my_col_cursor):
        rowCount += 1
        moclass_set.add(my_col_cursor.CurrentValue)
    if(len(moclass_set)>1):
        Document.Properties['MOClassName'] = ""
    else:
        if (len(moclass_set)==1):
            Document.Properties['MOClassName'] = list(moclass_set)[0]
    if(len(moclass_set)>=1):
        Document.Properties['moClassNameError'] =""
        
get_selected_values()