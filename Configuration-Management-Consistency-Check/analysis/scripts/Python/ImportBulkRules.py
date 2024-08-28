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
# Name    : ImportBulkRules.py
# Date    : 17/10/2023
# Revision: 1.0
# Purpose : Import rules from a csv file
#
# Usage   : CMCC Analysis
#
import ast
import clr
clr.AddReference('System.Data')
clr.AddReference("System.Windows.Forms")
from collections import OrderedDict
from System.Windows.Forms import OpenFileDialog,MessageBox
from System.Windows.Forms import OpenFileDialog,MessageBox
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from System.Data.Odbc import OdbcConnection, OdbcType
from System import Array, Byte
from System.IO import MemoryStream
from System.Text import UTF8Encoding

from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Framework.ApplicationModel import *

notify = Application.GetService[NotificationService]()

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)


def _from_bytes(bts):
    return [ord(b) for b in bts]


def _from_hex_digest(digest):
    return [int(digest[x:x+2], 16) for x in xrange(0, len(digest), 2)]


def decrypt(data, digest=True):
    '''
    Performs decrypting of provided encrypted data. 
    If 'digest' is True data must be hex digest, otherwise data should be
    encrtypted bytes.
    
    This function is symetrical with encrypt function.
    '''

    try:
        data = Array[Byte](map(Byte, _from_hex_digest(data) if digest else _from_bytes(data)))
        
        rm = RijndaelManaged()
        dec_transform = rm.CreateDecryptor(_key, _vector)
    
        mem = MemoryStream()
        cs = CryptoStream(mem, dec_transform, CryptoStreamMode.Write)
        cs.Write(data, 0, data.Length)
        cs.FlushFinalBlock()
        
        mem.Position = 0
        decrypted = Array.CreateInstance(Byte, mem.Length)
        mem.Read(decrypted, 0, decrypted.Length)
        
        cs.Close()
        utf_encoder = UTF8Encoding()
        return utf_encoder.GetString(decrypted)

    except Exception as e:
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))
        print("Exception: ", e)



def get_table_columns(table_name):
    '''
    Returns a list of the tables columns
    '''
    col_names = []

    table = Document.Data.Tables[table_name]
    for col in table.Columns:
        col_names.append(col.Name)
    return col_names


def create_table(table_name, ds):
    '''
    Creates a table in spotfire from the datasource
    '''
    if not Document.Data.Tables.Contains(table_name):
        Document.Data.Tables.Add(table_name, ds)
    else:
        Document.Data.Tables[table_name].ReplaceData(ds)


def insert_rule(imported_rules_table, rule_file_name):
    '''
    Read user inputs and adds the rule to the database table 
    '''
    paramater_list = {}

    batch_size = Document.Properties['BatchSize']
    print "insercet_rule"
    columns_for_insert_dict = OrderedDict(
        [
            ("RuleName", OdbcType.VarChar),
            ("MOClass", OdbcType.VarChar),
            ("CMAttribute", OdbcType.VarChar),
            ("ID", OdbcType.VarChar),
            ("VectorIndex", OdbcType.Int),
            ("RuleValue", OdbcType.VarChar),
            ("RuleComment", OdbcType.VarChar),
            ("WhereClause", OdbcType.VarChar),
            ("RuleSource", OdbcType.VarChar),
            ("ValidationStatus", OdbcType.VarChar),
            ("InvalidCauseDescription", OdbcType.VarChar),
            ("TableName", OdbcType.VarChar)
        ]
    )

    columns_for_insert = ["""\"{0}\"""".format(
        column) for column in columns_for_insert_dict]

    sql_query = """INSERT INTO "tblCMRules" ({0}) VALUES """.format(
        ','.join(columns_for_insert))

    rule_name = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["RuleName"])
    mo_class = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["MOClass"])
    cm_attribute = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["Attribute"])
    cm_id = DataValueCursor.CreateFormatted(imported_rules_table.Columns["ID"])
    vector_index = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["VectorIndex"])
    value = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["Value"])
    comment = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["Comment"])
    whereclause = DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["Where"])
    table_name=DataValueCursor.CreateFormatted(
        imported_rules_table.Columns["TableName"])
    parameter_index = 0
    for _ in imported_rules_table.GetRows(rule_name, mo_class, cm_attribute, cm_id, vector_index, value, comment, whereclause,table_name):

        RuleName = rule_name.CurrentValue
        MOClass = mo_class.CurrentValue
        CMAttribute = cm_attribute.CurrentValue
        TableName=table_name.CurrentValue
        if TableName == '(Empty)':
            TableName = None
        else:
            TableName = TableName
        if cm_id.CurrentValue == '(Empty)':
            cmID = None
        else:
            cmID = cm_id.CurrentValue
        if vector_index.CurrentValue == '(Empty)':
            VectorIndex = None
        else:
            try:
                VectorIndex = int(vector_index.CurrentValue)
            except Exception as e:
                print("Exception: ", e)
                notify.AddWarningNotification("Exception","Error in importing rule %s"%RuleName,str(e)) 
                break
        Value = value.CurrentValue
        if ',' in Value:
            Value=Value.replace(',','')
        if comment.CurrentValue == '(Empty)':
            Comment = None
        else:
            Comment = comment.CurrentValue
        if whereclause.CurrentValue == '(Empty)':
            WhereClause = None
        else:
            WhereClause = whereclause.CurrentValue

        paramater_list[parameter_index] = {
            'RuleName': RuleName,
            'MOClass': MOClass,
            'CMAttribute': CMAttribute,
            'ID': cmID,
            'VectorIndex': VectorIndex,
            'RuleValue': Value,
            'RuleComment': Comment,
            'WhereClause': WhereClause,
            'RuleSource': rule_file_name,
            'ValidationStatus': 'Not Validated',
            'InvalidCauseDescription': '',
            'TableName':TableName
        }

        parameter_index += 1
        print columns_for_insert_dict
        if parameter_index == batch_size:
            sql_query += create_value_list_for_sql(
                paramater_list, columns_for_insert)
            
            run_netan_sql_param(sql_query, paramater_list,
                                columns_for_insert_dict)
            sql_query = """INSERT INTO "tblCMRules" ({0}) VALUES """.format(
                ','.join(columns_for_insert))
            paramater_list.clear()
            parameter_index = 0

    if parameter_index > 0:
        sql_query += create_value_list_for_sql(
            paramater_list, columns_for_insert)
        print(sql_query)
        run_netan_sql_param(sql_query, paramater_list, columns_for_insert_dict)
        sql_query = """INSERT INTO "tblCMRules" ({0}) VALUES """.format(
            ','.join(columns_for_insert))


def create_value_list_for_sql(parameter_dict, column_list):
    """ 
    create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added.
    """
    overall_rows = []
    for _ in parameter_dict.items():
        value_list = []
        current_row = ""

        for _ in range(len(column_list)):
            value_list.append('?')

        current_row = """({0})""".format(','.join(value_list))
        overall_rows.append(current_row)

    return ','.join(overall_rows)


def run_netan_sql_param(sql, query_parameters, column_list):
    """ 
    Run a SQL query using ODBC connection 
    """
    conn_string = Document.Properties['ConnStringNetAnDB'].replace(
        "@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))
    try:

        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)
        command.ExecuteNonQuery()

        connection.Close()
    except Exception as e:
        print(e.message)
        raise


def apply_parameters(command, query_parameters, column_list):
    """ 
    for an ODBC command, adds all the required values for the parameters.
    """

    parameter_index = 0

    for col, col_value in query_parameters.items():
        # need to be added in correct order, so use the column_list to define the order
        for column_name, odbc_col_type in column_list.items():
            command.Parameters.Add(
                "@col"+str(parameter_index), odbc_col_type).Value = col_value[column_name]
            parameter_index += 1

    return command


def validate_file(data_table):
    '''
    Checks that the file has correct columns
    '''
    rule_table_columns = ['MOClass', 'ID', 'Attribute',
                          'VectorIndex', 'Value', 'Comment', 'Where', 'RuleName','TableName']

    for col in data_table.Columns:
        if col.Name not in rule_table_columns:
            return False

    return True

def get_file_from_dialog():
    file_dialog = OpenFileDialog()
    file_dialog.Multiselect = True
    file_dialog.InitialDirectory = 'C:\\'
    file_dialog.ShowDialog()
    files = file_dialog.FileNames
    ps.ExecuteWithProgress('Removing Duplicate Rules from Table ...','Removing Duplicate Rules',read_rules_from_files(files))
 

def read_rules_from_files():
    '''
    Reads the rules from the selected file and adds them to a imported rules table
    '''
    try:
        ps.CurrentProgress.ExecuteSubtask('Fetching rules from files ...')
        ps.CurrentProgress.CheckCancel()
        failed_file_names = []

        for cm_rule_file in files:

            rule_file_name = cm_rule_file.split('\\')[-1]
            ds = Document.Data.CreateFileDataSource(cm_rule_file)
            create_table(imported_rules_table_name, ds)
            imported_rules_table = Document.Data.Tables[imported_rules_table_name]

            if validate_file(imported_rules_table):
                try:
                    insert_rule(imported_rules_table, rule_file_name)
                except Exception as e:
                    print("Exception: ", e)
                    notify.AddWarningNotification("Exception","Error in importing rule",str(e)) 
            else:
                failed_file_names.append(rule_file_name)
        cmrules.Refresh()
        if len(failed_file_names) > 0:
            Document.Properties['ImportErrorMessage'] = 'Import of the following file(s) failed: ' + str(failed_file_names)
        else:
            Document.Properties['ImportErrorMessage'] = ''

        Document.Data.Tables.Remove(imported_rules_table)
        cmrules.Refresh()
        ps.CurrentProgress.ExecuteSubtask('Removing Duplicates ...')
        ps.CurrentProgress.CheckCancel()
        remove_duplicated_rules(cmrules)
    except ProgressCanceledException as pce:
        print("ProgressCanceledException: ", pce)
    except Exception as e:
        print("Exception: ", e)
        notify.AddWarningNotification("Exception","Error in DataBase Connection",str(e))          



def remove_duplicated_rules(data_table):
    rule_check_list = []
    rows_to_remove = []
    cursor = DataValueCursor.CreateFormatted( data_table.Columns['RuleID'])
    for row in data_table.GetRows(cursor):
        index = row.Index
        rule_id = data_table.Columns['RuleID'].RowValues.GetFormattedValue(index)
        mo_class = data_table.Columns['MOClass'].RowValues.GetFormattedValue(index)
        attribute = data_table.Columns['CMAttribute'].RowValues.GetFormattedValue(index)
        value = data_table.Columns['RuleValue'].RowValues.GetFormattedValue(index)
        rule_name = data_table.Columns['RuleName'].RowValues.GetFormattedValue(index)
        table_name = data_table.Columns['TableName'].RowValues.GetFormattedValue(index)
        check = [mo_class,attribute,value,rule_name,table_name]
        match = [sublist for sublist in rule_check_list if sublist == check]
        if match:
            print('Duplicate rule found: %s' % index)
            rows_to_remove.append(int(rule_id))

        else:
            rule_check_list.append([mo_class,attribute,value,rule_name,table_name])
    delete_rows(rows_to_remove,data_table)


def delete_rows(rows_to_remove, data_table):
    """
    Deletes rules 

    """
    batch_size = Document.Properties['BatchSize']
    parameter_list = {}
    parameter_index = 0
    columns_for_insert = {"RuleID": OdbcType.Int}
    for row_id in rows_to_remove:
        parameter_list[parameter_index] = {"RuleID": row_id}
        parameter_index += 1
        if parameter_index == batch_size:
            sql = 'Delete from "tblCMRules" where "RuleID" IN ( '
            sql += create_value_list_for_sql(parameter_list, columns_for_insert)
            sql += ' )'
            write_delete_from_db(sql, parameter_list, columns_for_insert)
            parameter_list.clear()
            parameter_index = 0
            print(sql)
    if parameter_index > 0:
        sql = 'Delete from "tblCMRules" where "RuleID" IN ( '
        sql += create_value_list_for_sql(parameter_list, columns_for_insert)
        sql += ' )'
        write_delete_from_db(sql, parameter_list, columns_for_insert)
        
        
def write_delete_from_db(sql, query_parameters, column_list):
    """
    Executes sql query in NetAn db to delete rule

    Returns:
        {boolean} -- true or false based on if sql staetement executed successfully or not
    """

    conn_string = Document.Properties['ConnStringNetAnDB'].replace(
        "@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)
        command.ExecuteNonQuery()
        connection.Close()
    except Exception as e:
        print(e.message)
        raise


cmrules = Document.Data.Tables['cmrules']
imported_rules_table_name = 'BulkImportRules'
ps = Application.GetService[ProgressService]()
file_dialog = OpenFileDialog()
file_dialog.Multiselect = True
file_dialog.InitialDirectory = 'C:\\'
file_dialog.ShowDialog()
files = file_dialog.FileNames
if len(files)>0:
    ps.ExecuteWithProgress('Fetching Rules from File ...','Adding Rules to Table',read_rules_from_files)
    cmrules.Refresh()
else:
    MessageBox.Show("No file selected, please try again!")
  
