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
# Name    : SaveEditRule.py
# Date    : 16/10/2023
# Revision: 1.0
# Purpose : Edit rule inputs, validates them and Saves the rule to the rule table
#
# Usage   : CMCC Analysis

import ast
import re
import clr
clr.AddReference('System.Data')
from collections import OrderedDict
from System.Data.Odbc import OdbcConnection, OdbcType
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode
from System.IO import MemoryStream
from System import Array, Byte
from System.Text import UTF8Encoding

from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Data import DataValueCursor, IndexSet

notify = Application.GetService[NotificationService]()

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)

class Attribute():
    MOClass = 'MOClass'
    Attribute = 'Attribute'
    TableName = 'TableName'
    ENIQDataType = 'ENIQDataType'

def _from_bytes(bts):
    return [ord(b) for b in bts]


def _from_hex_digest(digest):
    return [int(digest[x:x+2], 16) for x in xrange(0, len(digest), 2)]

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


def nav_to_rule_manager():
    """
    Navigate back the the rule manager page
    """
    for page in Document.Pages:
          if (page.Title == "CM Rule Manager"):
              Document.ActivePageReference=page


def validate_required_fields(rule_name, mo_class, wanted_value, attribute):
    '''
    Checks that required fields are filled
    '''
    error = ''
    if rule_name == '' or mo_class == '' or wanted_value == '' or attribute == '':
        error = 'Please enter required fields.'

    return error

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
    check= tablename.upper()[-2:]
    error = ''
    if vector_index!='(Empty)' and vector_index!='':
        if check=='_V':
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

def validate_rule():
    '''
    The MOClass, Attribute, Value type and Where Condition is validated
    The validation status and cause is returned
    '''
    selected_date = Document.Properties["SelectedDate"].ToString('yyy-MM-dd')
    data_source_name = Document.Properties["DataSourceName"]
    database_connection_result = 'DatabaseConnectionResult'
    data_table = Document.Data.Tables['cmrules']

    attribute_type, attribute_table, mo_classes = fetch_mo_attribute_details()

    mo_class = Document.Properties['MOClassName'].strip().upper()
    attribute = Document.Properties['AttributeName'].strip().upper()
    where_condition = Document.Properties['WhereConditionName'].strip()
    wanted_value = Document.Properties['ValueName'].strip()
    rule_name = Document.Properties['RuleName'].strip()
    vector_index = Document.Properties['VectorIndex'].strip()
    tableName= Document.Properties['TableName'].strip()
    mo_tuple = (mo_class.lower(), attribute.lower())
    wanted_value_type = attribute_type[mo_tuple] if mo_tuple in attribute_type else 'String'
    try:
        error_list = []
        error_list.append(validate_required_fields(
            rule_name, mo_class, wanted_value, attribute))
        error_list.append(validate_mo_class(mo_class, mo_classes))
        error_list.append(validate_attribute(
            mo_tuple, attribute_table, mo_class, mo_classes))
        error_list.append(validate_where_condition(where_condition))
        error_list.append(validate_value(wanted_value, wanted_value_type))
        error_list.append(check_for_duplicated_rule(data_table))
        error_list.append(validate_multiple_table(tableName,list(attribute_table[mo_tuple])))
        finaltablename=''
        if (tableName == '' or tableName == '(EMPTY)') and len(list(attribute_table[mo_tuple]))==1:
            finaltablename=list(attribute_table[mo_tuple])[0]
        else:
            finaltablename=tableName
        error_list.append(validate_vector_index(vector_index,finaltablename))
    except Exception as e:
                print "error"            
    errors = [err for err in error_list if err != '']
    print(errors)
    
    if len(errors) == 0 and where_condition != '':
        sql_invalid, sql_cause = validate_sql(
            rule_name, mo_class, attribute, finaltablename, selected_date, data_source_name, database_connection_result)
        print "SQL",sql_invalid, sql_cause
        if sql_invalid == True:
            errors.append('Invalid SQL ' + str(sql_cause))
    return errors

def fetch_data_from_ENIQ_async(sql, table_name, data_source_name, database_connection_result):
    '''
    Execute SQL query to get data from ENIQ
    '''
    try:
        data_source_settings = DatabaseDataSourceSettings(
            "System.Data.Odbc", "DSN=" + data_source_name, sql)
        data_table_data_source = DatabaseDataSource(data_source_settings)
        if Document.Data.Tables.Contains(table_name):
            Document.Data.Tables[table_name].ReplaceData(
                data_table_data_source)
        else:
            Document.Data.Tables.Add(table_name, data_table_data_source)
        Document.Properties[database_connection_result] = 'Validated'
    except Exception as e:
        print('Exception: ', e)
        Document.Properties[database_connection_result] = str(e)

def get_excluded_nodes():
    '''
    Get nodes which user has added to excluded nodes list
    '''
    excluded = []
    excluded_table = Document.Data.Tables['tblExcludedNodes']
    cursor_node_name = DataValueCursor.CreateFormatted(
        excluded_table.Columns["NodeName"])
    excluded_rows = IndexSet(excluded_table.RowCount, True)
    for _ in excluded_table.GetRows(excluded_rows, cursor_node_name):
        excluded.append(cursor_node_name.CurrentValue)

    return "('%s')" % "','".join(excluded)


def validate_sql(rule_name, moc, attr, attribute_table, selected_date, data_source_name, database_connection_result):
    '''
    Validates the SQL query. This must be valid 
    '''
    print "ini"
    invalid_rule_found = False
    validate_table_name = 'Validate SQL'
    comparison_test_format = '(%s<>%s)'
    excluded_nodes_list = get_excluded_nodes()
    wanted_value = Document.Properties['ValueName']
    identity = ''
    vector_index = Document.Properties['VectorIndex']
    where_condition = Document.Properties['WhereConditionName']

    try:
        wanted_value = int(wanted_value)
        attr_str = "str(%s)" % attr
    except:
        attr_str = attr
        if '=' in wanted_value:
            comparison_test_format = "(%s not like '%%%s')"
        else:
            comparison_test_format = "(%s<>'%s')"

    identity = identity if identity != '(Empty)' else ''
    vector_index = vector_index if vector_index != '(Empty)' else ''
    where_condition = where_condition if where_condition != '(Empty)' else ''

    sql_template = "SELECT DATE_ID, ELEMENT, OSS_ID, '%s' as Rule, SN + ',' + MOID as FDN, '%s' as MOClass, '%s' as ID, '%s' as Attribute, '%s' as VectorIndex, %s as CurrentValue FROM %s_RAW WHERE "
    sql = sql_template % (rule_name, moc, identity, attr, vector_index,
                          attr_str, attribute_table)
    sql = sql + comparison_test_format % (attr, wanted_value)
    print "1SQL",sql
    if identity != '':
        sql = sql + " and MOID like '%%%s=%s'" % (moc, identity)

    if vector_index != '':
        sql = sql + " and DCVECTOR_INDEX=%s" % vector_index

    sql = sql + \
        " and DATE_ID='%s' and ELEMENT NOT IN %s " % (
            selected_date, excluded_nodes_list)

    if where_condition != '':
        where_condition = re.sub(
            'where', 'where', where_condition, flags=re.IGNORECASE)
        sql = sql + ' and ' + \
            where_condition.replace(
                "where", "where DATE_ID='%s' and " % selected_date)

    original_sql = sql
    print "SQL",sql
    fetch_data_from_ENIQ_async(
        sql, validate_table_name, data_source_name, database_connection_result)
    result = Document.Properties[database_connection_result]

    if result.startswith('Failed'):
        invalid_rule_found = True
        result = result + '; SQL="%s"' % original_sql
    print "ii",invalid_rule_found, result
    return invalid_rule_found, result

def generate_update_sql():
    """
    Creates sql statement template for the report with a column list.
      
    Returns:
        sql {String} -- sql statement template
        columns_for_insert_dict {list}
    """

    sql = """
            UPDATE "tblCMRules" 
            SET 
                    "RuleName" = ?,
                    "MOClass" = ?,
                    "CMAttribute" = ?,
                    "ID" = ?,
                    "VectorIndex" = ?,
                    "RuleValue" = ?,
                    "RuleComment" = ?,
                    "WhereClause" = ?,
                    "RuleSource" = ?,
                    "ValidationStatus" = ?,
                    "InvalidCauseDescription" = ?,
                    "TableName"=?
            WHERE "RuleID" = ?;
            """    

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
            ("TableName",OdbcType.VarChar),
            ("RuleID",OdbcType.Int)
            
        ]
    )

    return sql,columns_for_insert_dict


def write_to_db(sql, query_parameters, column_list):
    """
    Executes sql query in NetAn db to insert/delete reports

    Arguments:
        sql {String} -- sql statement template
        
    Returns:
        {boolean} -- true or false based on if sql staetement executed successfully or not
    """
    conn_string = Document.Properties['ConnStringNetAnDB'].replace("@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command = apply_parameters(command, query_parameters, column_list)
        command.ExecuteNonQuery()
        connection.Close()
        return True
    except Exception as e:
        print (e.message)
        return False


def apply_parameters(command, query_parameters, column_list):
    """ for an ODBC command, add all the required values for the parameters."""

    parameter_index = 0

    for col,col_value in query_parameters.items():
        # need to be added in correct order, so use the column_list to define the order
        for column_name, odbc_col_type in column_list.items(): 
            command.Parameters.Add("@col"+str(parameter_index), odbc_col_type).Value = col_value[column_name]
            parameter_index += 1

    return command

def edit_rule():

    paramater_list = {}

    rule_name = Document.Properties["RuleName"].strip()
    mo_class_name = Document.Properties["MOClassName"].strip()
    attribute_name = Document.Properties["AttributeName"].strip()
    id_name = Document.Properties["IDName"].strip()
    value_name = Document.Properties["ValueName"].strip()
    vector_index = Document.Properties["VectorIndex"].strip()
    where_condition_name = Document.Properties["WhereConditionName"].strip()
    comment_name = Document.Properties["CommentName"].strip()
    source_name = Document.Properties["RuleManagerPageName"].strip()
    ruleid = int(Document.Properties["ruleid"])
    tablename=Document.Properties["TableName"].strip()
    if vector_index == '':
        vector_index_value = None
    else:
        vector_index_value = int(vector_index)

    paramater_list['update_query'] = {
    'RuleName': rule_name,
    'MOClass': mo_class_name,
    'CMAttribute': attribute_name,
    'ID': id_name,
    'VectorIndex': vector_index_value,
    'RuleValue': value_name,
    'RuleComment': comment_name,
    'WhereClause': where_condition_name,
    'RuleSource': source_name,
    'ValidationStatus': 'Valid',
    'InvalidCauseDescription': '',
    'RuleID': ruleid,
    'TableName':tablename
    }

    sql,column_list = generate_update_sql()
    if(write_to_db(sql,paramater_list,column_list)):
         cmrules = Document.Data.Tables['cmrules']
         cmrules.Refresh()
         nav_to_rule_manager()
    else:
         Document.Properties['CreateRuleError'] = "error"

def cleanup():
    Document.Properties["RuleName"] = ''
    Document.Properties["MOClassName"] = ''
    Document.Properties["AttributeName"] = ''
    Document.Properties["IDName"] = ''
    Document.Properties["ValueName"] = ''
    Document.Properties["VectorIndex"] = ''
    Document.Properties["WhereConditionName"] = ''
    Document.Properties["CommentName"] = ''
    Document.Properties["ruleid"] = ''
    Document.Properties["TableName"] = ''

def main():
    """
    main flow
    """
    errors = validate_rule()
    Document.Properties['CreateRuleError'] = ', '.join(errors)
    print "error",errors
    if len(errors) == 0:
        edit_rule()
        cleanup()

main()
