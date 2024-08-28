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
# Name    : CheckRules.py
# Date    : 17/10/2023
# Revision: 1.0
# Purpose : Executes valid rules to get MO counts and discrepancy data
#
# Usage   : CMCC Analysis
#
import clr
from Spotfire.Dxp.Data import DataValueCursor, IndexSet, AddRowsSettings, RowSelection
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource
from Spotfire.Dxp.Data import DataColumnSignature
from Spotfire.Dxp.Data.Transformations import ColumnAggregation, PivotTransformation, ReplaceColumnTransformation
from Spotfire.Dxp.Framework.ApplicationModel import ProgressService, ProgressCanceledException, NotificationService
from System import Array, Byte
clr.AddReference("System.Windows.Forms")
import re
import time
from System.Windows.Forms import OpenFileDialog, MessageBox, DialogResult, MessageBoxButtons
start_time = time.time()

def get_list_of_selected_rules():
    """
    Generates list of user selected rule ids from Rules with Status data table

    Returns:
        selected_rules {list} -- list of rule names selected by the user
    """
    rulename=[]
    data_table = Document.Data.Tables['cmrules']
    rows = IndexSet(data_table.RowCount, True)
    columns = ["ValidationStatus","RuleName"]
    cursors = {column: DataValueCursor.CreateFormatted(
        data_table.Columns[column]) for column in columns}
    markings = markings = Document.Data.Markings['MarkingRules'].GetSelection(data_table)
    for _ in data_table.GetRows(markings.AsIndexSet(), Array[DataValueCursor](cursors.values())):
        print "xx",cursors["ValidationStatus"].CurrentValue
        print "yy",cursors["RuleName"].CurrentValue
        if(cursors["ValidationStatus"].CurrentValue=='Invalid'):
            rulename.append(cursors["RuleName"].CurrentValue)
    print "selected_rules",rulename
    return rulename

x=get_list_of_selected_rules()

if len(x)>0:
	Document.Properties['TriggerExecuteRule']="TRUE"