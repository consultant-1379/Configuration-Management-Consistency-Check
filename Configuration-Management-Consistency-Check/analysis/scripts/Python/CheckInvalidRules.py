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
# Name    : CheckInvalidRules.py
# Date    : 28/09/2023
# Revision: 1.0
# Purpose : Determines if rules are valid/invalid and stores results in Rules with Status table
#
# Usage   : CMCC Analysis
#

import clr
clr.AddReference('System.Data')
from collections import OrderedDict
from Spotfire.Dxp.Data import DataValueCursor, IndexSet
from Spotfire.Dxp.Data.Import import TextDataReaderSettings, TextFileDataSource
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, TextDataReaderSettings, TextFileDataSource
from Spotfire.Dxp.Framework.ApplicationModel import ProgressService, ProgressCanceledException, NotificationService

from System.Text import UTF8Encoding
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from System.Data.Odbc import OdbcConnection, OdbcType
from System.IO import MemoryStream
from System import Array, Byte
import re
import time
import ast
start_time = time.time()

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

selected_date = Document.Properties["SelectedDate"].ToString('yyy-MM-dd')
data_source_name = Document.Properties["DataSourceName"]
database_connection_result = "DatabaseConnectionResult"
rules_marking_name = 'MarkingRules'
application_type = Application.GetType().ToString()
analysis_client = 'Spotfire.Dxp.Application.RichAnalysisApplication'
notify = Application.GetService[NotificationService]()

rules_table_name = 'cmrules'
excluded_nodes_table_name = 'tblExcludedNodes'
rules_table = Document.Data.Tables[rules_table_name]

class Rule():
    RuleID = 'RuleID'
    MOClass = 'MOClass'
    Attribute = 'CMAttribute'
    ID = 'ID'
    Value = 'RuleValue'
    VectorIndex = 'VectorIndex'
    Where = 'WhereClause'
    RuleName = 'RuleName'
    TableName = 'TableName'


class Attribute():
    MOClass = 'MOClass'
    Attribute = 'Attribute'
    TableName = 'TableName'
    ENIQDataType = 'ENIQDataType'


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


def get_rules_rows(rules_table):
    '''
     Get marked rules, else return all rules
    '''
    rules_rows = Document.Data.Markings[rules_marking_name].GetSelection(rules_table).AsIndexSet()

    if rules_rows.Count == 0:
        rules_rows = IndexSet(rules_table.RowCount, True)
    return rules_rows


def set_null_to_empty_string(string_to_empty):
    '''
    Sets Null string values (Empty) to empty string ''
    '''
    return string_to_empty if string_to_empty != '(Empty)' else ''

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
    
        
def fetch_mo_attribute_details():
    '''
    Gets the CM Attribute details which will be used to validate rule inputs
    '''

    data_table = Document.Data.Tables['CM Attributes']
    rows = IndexSet(data_table.RowCount, True)
    columns = [Attribute.MOClass, Attribute.Attribute, Attribute.TableName, Attribute.ENIQDataType]
    cursors = {column: DataValueCursor.CreateFormatted(
        data_table.Columns[column]) for column in columns}
    records = []
    for _ in data_table.GetRows(rows, Array[DataValueCursor](cursors.values())):
        records.append(
            tuple([cursors[column].CurrentValue for column in columns]))

    attribute_table = {}
    attribute_type = {}

    mo_classes = set()
    for mo_class, attribute, bulk_cm_table, eniq_datatype in records:
        mo_classes.add(mo_class.upper())

        datatype = 'String'
        if 'int' in eniq_datatype:
            datatype = 'Integer'
        mo_tuple = (mo_class.lower(), attribute.lower())
        if mo_tuple not in attribute_type:
            attribute_type[mo_tuple] = datatype
            attribute_table[mo_tuple] =set()
            attribute_table[mo_tuple].add(bulk_cm_table)
        else:
            attribute_table[mo_tuple].add(bulk_cm_table)

    return attribute_type, attribute_table, mo_classes


def validate_rule_name(rule_name):
    '''
    Checks for empty rule name
    '''
    return 'Invalid Rule Name' if rule_name == '' or rule_name=='(Empty)' else ''


def validate_mo_class(mo_class, mo_classes):
    '''
    checks that the rule MO Class exists
    '''
    return 'Invalid MO Class' if mo_class not in mo_classes else ''



def validate_attribute(mo_tuple, attribute_table, mo_class, mo_classes):
    '''
    checks if the cm attribute exists
    '''
    error = ''
    if mo_tuple not in attribute_table:
        if mo_class in mo_classes:
            error = 'Invalid Attribute'

    return error


def validate_where_condition(where_condition):
    '''
    validates the rule's where clause
    '''
    error = ''
    if where_condition and (where_condition.isalpha()
                    or not ('=' in where_condition
                    or '<>' in where_condition
                    or 'like' in where_condition.lower()
                    or ' in ' in where_condition.lower())):
        error = 'Missing conditional operator in Where clause'
    else:
        error = ''
    
    return error


def validate_value(wanted_value, wanted_value_type):
    '''
    Validates the rule value and value type
    '''
    error = ''
    if wanted_value.lower() == 'null' or wanted_value.lower() == '(empty)':
        error= 'Invalid Rule Value' 
        pass

    if wanted_value_type == 'String':
        try:
            value = int(wanted_value)
            error= 'String data type with Invalid Value: %s' % wanted_value
        except Exception as e:
            print('Exception: ', e)
    elif wanted_value_type == 'Integer':
        try:
            value = int(wanted_value)
        except Exception as e:
            print('Exception: ', e)
            error= 'Integer data type with Invalid Value: %s' % wanted_value
    
    return error


def validate_vector_index(vector_index,tablename):
    '''
    Verifies that vector index is an int
    '''
    print "ta",vector_index
    print "tablename",tablename
    check= tablename.upper()[-2:]
    error = ''
    print "check",check
    if vector_index!='(Empty)' and vector_index!='':
        if check=='_V':
            print "yes"
            try:
                value = int(vector_index)
            except Exception as e:
                print('Exception: ', e)
                error = 'Invalid vector index: %s' % vector_index
        else:
            error = 'Invalid vector index'
    return error


def validate_multiple_table(tablename,tablelist):
    '''
    Verifies that unique combination of Attribute and MOClass returns only one table or not.
    '''
    error = ''
    if tablename == '' or tablename =='(EMPTY)':
        if len(tablelist)>1:
            error = "MOclass and attribute maps to following ENIQ tables: {0}. Enter TableName field to specify one of these table names.".format(", ".join(tablelist))
    elif tablename not in tablelist:
        error = "Enter correct ENIQ table Name. Possible ENIQ tables:{0}".format(",  ".join(tablelist))   
    return error
    
def validate_rules(rules_table, attribute_type, attribute_table, mo_classes):
    '''
    For each rule, the MOClass, Attribute, Value type and Where Condition is validated
    The status for each rule is stored in a set and returned

    Returns
    rules_status (set)
    '''
    rules_validation_dict = {}
    data_table = rules_table
    rules_rows = get_rules_rows(rules_table)
    try:   
        columns = [Rule.RuleID, Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName, Rule.TableName]
        cursors = {column: DataValueCursor.CreateFormatted(data_table.Columns[column]) for column in columns}
        if application_type == analysis_client:
            ps.CurrentProgress.ExecuteSubtask(progress_text)
            ps.CurrentProgress.CheckCancel()

        for _ in data_table.GetRows(rules_rows, Array[DataValueCursor](cursors.values())):
            rule_id = int(cursors[Rule.RuleID].CurrentValue)
            mo_class = cursors[Rule.MOClass].CurrentValue.strip().upper()
            attribute = cursors[Rule.Attribute].CurrentValue.strip().upper()
            tableName = cursors[Rule.TableName].CurrentValue.strip().upper()
            where_condition = set_null_to_empty_string(cursors[Rule.Where].CurrentValue.strip())
            wanted_value = cursors[Rule.Value].CurrentValue.strip()
            rule_name = cursors[Rule.RuleName].CurrentValue.strip()
            vector_index = cursors[Rule.VectorIndex].CurrentValue.strip()
            identity = set_null_to_empty_string(cursors[Rule.ID].CurrentValue.strip())
            mo_tuple = (mo_class.lower(), attribute.lower())
            wanted_value_type = attribute_type[mo_tuple] if mo_tuple in attribute_type else 'String'
            error_list = []
            try:
                error_list.append(validate_rule_name(rule_name))
                error_list.append(validate_attribute(mo_tuple, attribute_table, mo_class, mo_classes)) 
                error_list.append(validate_mo_class(mo_class, mo_classes))
                error_list.append(validate_value(wanted_value, wanted_value_type))
                error_list.append(validate_multiple_table(tableName,list(attribute_table[mo_tuple])))
                error_list.append(validate_where_condition(where_condition))
                finaltablename=''
                print "x",tableName
                if (tableName == '' or tableName == '(EMPTY)') and len(list(attribute_table[mo_tuple]))==1:
                    print "if"
                    finaltablename=list(attribute_table[mo_tuple])[0]
                else:
                    print "else"
                    finaltablename=tableName
                error_list.append(validate_vector_index(vector_index,finaltablename))
            except Exception as e:
                print "error"
            errors = [err for err in error_list if err != ''] 
            if len(errors) == 0 and where_condition != '':
                sql_invalid, sql_cause = validate_sql(rule_name, mo_class, attribute, wanted_value, vector_index, where_condition, identity,finaltablename)

                if sql_invalid == True:
                    errors.append('Invalid SQL')

            error_str = ''
            if len(errors) > 0:
                error_str = ', '.join(str(error) for error in errors)
                validation_status = 'Invalid'
            else:
                validation_status = 'Valid'
            
            rule_validation_dict = {
            'validation_status':validation_status,
            'invalid_cause_description':error_str
            }

            rules_validation_dict[rule_id] = rule_validation_dict

    except ProgressCanceledException as pce:
        print("ProgressCanceledException: ", pce)
        rule_validation_dict = {
            'validation_status':"Invalid",
            'invalid_cause_description':"Invalid Parameters"
            }

    return rules_validation_dict
    
def validate_sql(rule_name, moc, attr, wanted_value, vector_index, where_condition, identity,tablename):
    '''
    Validates the SQL query. This must be valid 
    ''' 
    selected_datevalue = Document.Properties["SelectedDate"].ToString('yyy-MM-dd')
    invalid_rule_found = False
    comparison_test_format = '(%s<>%s)'
    excluded_nodes_list = get_excluded_nodes(excluded_nodes_table_name)
    identity = ''

    try:
        wanted_value = int(wanted_value)
        attr_str = "str(%s)" % attr
    except:
        attr_str = attr
        if '=' in wanted_value:
            comparison_test_format = "(%s not like '%%%s')"
        else:
            comparison_test_format = "(%s<>'%s')"
    if "'" in rule_name:
        rule_name = rule_name.replace("'", '"')

    identity = identity if identity != '(Empty)' else ''
    vector_index = vector_index if vector_index != '(Empty)' else ''
    where_condition = where_condition if where_condition != '(Empty)' else ''
    sql_template = "SELECT DATE_ID, ELEMENT, OSS_ID, '%s' as Rule, SN + ',' + MOID as FDN, '%s' as MOClass, '%s' as ID, '%s' as Attribute, '%s' as VectorIndex, %s as CurrentValue FROM %s_RAW WHERE "
    sql = sql_template % (rule_name, moc, identity, attr, vector_index, attr_str, tablename)
    sql = sql + comparison_test_format % (attr, wanted_value)

    if identity != '':
        sql = sql + " and MOID like '%%%s=%s'" % (moc, identity)

    if vector_index != '':
        sql = sql + " and DCVECTOR_INDEX=%s" % vector_index

    sql = sql + " and DATE_ID='%s' and ELEMENT NOT IN %s " % (selected_datevalue, excluded_nodes_list)

    if where_condition != '':
        where_condition = re.sub('where', 'where', where_condition, flags=re.IGNORECASE)
        sql = sql + " and " + where_condition.replace("where", "where DATE_ID='%s' and " % selected_datevalue)

    result = ''
    result=fetch_data_from_ENIQ_async(sql)
    if result.startswith('Failed'):
        invalid_rule_found = True
        result = result + '; SQL="%s"' % sql

    return invalid_rule_found, result


def fetch_data_from_ENIQ_async(sql):
    '''
    Execute SQL query to get data from ENIQ
    '''
    data_source_name=Document.Properties["DataSourceName"]
    table_name='Validate SQL'
    try:       
        data_source_settings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + data_source_name, sql)
        data_table_data_source = DatabaseDataSource(data_source_settings)
        data_table_data_source.IsPromptingAllowed = False

        if Document.Data.Tables.Contains(table_name): 
            Document.Data.Tables[table_name].ReplaceData(data_table_data_source)
        else:
            Document.Data.Tables.Add(table_name, data_table_data_source)
        return 'Validated'

    except Exception as e:
        return 'Failed to execute SQL query: ' + str(e)



def save_rule_validation(rule_validation_dict):
    """
    Sets up parameterized query and parameters to update validationstatus and invalidcausedescription

    Arguments:
        rule_validation_dict {dictionary} -- dictionary containing new validationstatus and invalidcausedescription for each rule
      
    Returns:
        validation_saved {bool} -- boolean indicating if updates were successful
    """

    parameter_list = {}

    sql_query = '''
                UPDATE "tblCMRules"
                SET "ValidationStatus" = ?,
                "InvalidCauseDescription" = ? 
                WHERE "RuleID" = ?;
                '''
    columns_for_insert_dict = OrderedDict(
        [ 
            ("ValidationStatus", OdbcType.VarChar),
            ("InvalidCauseDescription",OdbcType.VarChar),
            ("RuleID", OdbcType.Int)
        ]
    )

    for rule_id in rule_validation_dict:
        rule_dict = rule_validation_dict[rule_id]
        validation_status = rule_dict['validation_status']
        invalid_cause_description = rule_dict['invalid_cause_description']

        parameter_list['update_query'] = {"ValidationStatus": validation_status, "InvalidCauseDescription": invalid_cause_description, "RuleID": rule_id}        
        
        validation_saved = write_delete_to_db(sql_query, parameter_list, columns_for_insert_dict)

    return validation_saved          


def write_delete_to_db(sql, query_parameters, column_list):
    """ Run a SQL query using ODBC connection """

    conn_string = Document.Properties['ConnStringNetAnDB'].replace(
        "@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)

        command.ExecuteReader()
        connection.Close()
        return True
    except Exception as e:
        print ("Exception: ",  str(e))
        return False
    

def apply_parameters(command, query_parameters, column_list):
    """ 
    for an ODBC command, add all the required values for the parameters.
    """

    parameter_index = 0

    for col, col_value in query_parameters.items():

        for column_name, odbc_col_type in column_list.items():
            command.Parameters.Add(
                "@col"+str(parameter_index), odbc_col_type).Value = col_value[column_name]
            parameter_index += 1

    return command


def get_excluded_nodes(excluded_nodes_table_name):
    '''
    Get nodes which user has added to excluded nodes list
    '''
    excluded_list = []
    excluded_list_table = Document.Data.Tables[excluded_nodes_table_name]
    cursor_node_name = DataValueCursor.CreateFormatted(excluded_list_table.Columns["NodeName"])
    excluded_list_rows = IndexSet(excluded_list_table.RowCount, True)

    for _ in excluded_list_table.GetRows(excluded_list_rows, cursor_node_name):
        excluded_list.append(cursor_node_name.CurrentValue)

    return "('%s')" % "','".join(excluded_list)


def main():
    try:  
        attribute_type, attribute_table, mo_classes=fetch_mo_attribute_details()
        rules_validation_dict = validate_rules(rules_table,attribute_type, attribute_table, mo_classes)
        validation_saved = save_rule_validation(rules_validation_dict)

        if validation_saved:                            
            rules_table.Refresh()
        else:
            notify.AddWarningNotification("Exception","Error in DataBase Connection","Error in Saving the report")
    
    except Exception as e:
        print("Exception: ", str(e))


Rule = Rule()
Attribute = Attribute()

progress_text = 'Checking rule validity'
if application_type == analysis_client:
    ps = Application.GetService[ProgressService]()
    ps.ExecuteWithProgress(progress_text, 'Validating rules', main)
else:
    main()
