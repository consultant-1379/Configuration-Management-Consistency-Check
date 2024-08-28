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
#old
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

"""
Spotfire Properties:
"""
batch_size = Document.Properties["BatchSize"]
selected_date = Document.Properties["SelectedDate"].ToString('yyy-MM-dd')
data_source_name = Document.Properties["DataSourceName"]
calculate_percentage_discrepancies = Document.Properties["CalculatePercentageDiscrepancies"]
query_result = "QueryResult"
database_connection_result = "DatabaseConnectionResult"
rules_marking_name = 'MarkingRules'

"""
Scheduling, fetch Platform Info
"""

application_type = Application.GetType().ToString()
analysis_client = 'Spotfire.Dxp.Application.RichAnalysisApplication'
notify = Application.GetService[NotificationService]()

"""
Tables
"""
rules_table_name = 'cmrules'
excluded_table_name = 'tblExcludedNodes'
mapping_table_name = 'CM Attributes'
discrepancies_table_name = 'Discrepancies'
invalid_rules_table_name = 'Invalid Rules'
validation_status_table_name = 'Validation Status'

invalid_cause_description_column_name = 'Invalid Cause Description'

rules_table         = Document.Data.Tables[rules_table_name]
discrepancies_table = Document.Data.Tables[discrepancies_table_name]

num_valid_rules = 0
num_invalid_rules = 0

class Rule():
    MOClass = 'MOClass'
    Attribute = 'CMAttribute'
    ID = 'MOInstance'
    Value = 'RuleValue'
    VectorIndex = 'VectorIndex'
    Where = 'WhereClause'
    RuleName = 'RuleName'
    WhereCondition = 'WhereClause'
    ValidationStatus = 'ValidationStatus'
    TableName='TableName'


class Attribute():
    MOClass = 'MOClass'
    Attribute = 'Attribute'
    TableName = 'TableName'
    ENIQDataType = 'ENIQDataType'

def get_excluded_nodes(excluded_table_name):
    '''
    Gets excluded nodes from table
    '''
    excluded_list = []
    excluded_table = Document.Data.Tables[excluded_table_name]
    cursor_node_name = DataValueCursor.CreateFormatted(excluded_table.Columns["NodeName"])
    excluded_rows = IndexSet(excluded_table.RowCount, True)
    for node in excluded_table.GetRows(excluded_rows, cursor_node_name):
        excluded_list.append(cursor_node_name.CurrentValue)

    return "('%s')" % "','".join(excluded_list)


def divide_into_batches(list_name, n):
    '''
    Splits a list into batches so that resulting queries don't exceed IQ SQL limits, e.g. can't have more than 512 tables in a single query 
    (SQL Anywhere Error -1001030: Feature, More than 512 tables in a query, is not supported.)
    '''
    for i in range(0, len(list_name), n):
        yield list_name[i:i+n]


def get_rules_rows(rules_table):
    '''
    Gets marked rules
    If no rules are marked, return all rules
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



def clear_existing_discrepancies():
    discrepancies_table = Document.Data.Tables['Discrepancies']
    discrepancies_table.RemoveRows(RowSelection(IndexSet(discrepancies_table.RowCount,True)))

    discrepancies_stats_table = Document.Data.Tables['Discrepancies (Statistics)']
    discrepancies_stats_table.RemoveRows(RowSelection(IndexSet(discrepancies_stats_table.RowCount, True)))


def fetch_data_from_ENIQ(discrepancy_queries, data_source_name, result_table):
    '''
    Fetch the MO counts from ENIQ
    '''
    replace_result_table_on_first_query = [True]
    result_table_settings = AddRowsSettings(result_table, DataTableDataSource(result_table))
    ps = Application.GetService[ProgressService]()
    sql_queries = sorted(discrepancy_queries.keys())

    def fetch_data_from_eniq_async():
        '''
        This callback function cannot take parameters, so variables used are globals
        '''
        try:
            ps.CurrentProgress.ExecuteSubtask("Fetching discrepancies from %s ..." % data_source_name)
            data_source_settings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + data_source_name, sql)
            data_table_data_source = DatabaseDataSource(data_source_settings)
            if True in replace_result_table_on_first_query and replace_result_table_on_first_query.pop():
                result_table.ReplaceData(data_table_data_source)
            else:
                result_table.AddRows(data_table_data_source, result_table_settings)
            ps.CurrentProgress.CheckCancel()
            Document.Properties[query_result] = 'OK'
        except ProgressCanceledException as pce:
            print("ProgressCanceledException: ", pce)
            Document.Properties[query_result] = 'User cancelled'
        except Exception as e:
            Document.Properties[query_result] = 'Failed with exception: %s' % str(e)
            notify.AddWarningNotification("Exception","Error in executing rules", str(e))

    discrepancy_batch_size = batch_size
    discrepancy_batch_queries = divide_into_batches(sql_queries, discrepancy_batch_size)
    number_of_batches = int(len(discrepancy_queries)/discrepancy_batch_size) + 1
    discrepancy_batch_count = 0
    for discrepancy_batch_query in discrepancy_batch_queries:
        sql = ' union all '.join(discrepancy_batch_query)
        discrepancy_batch_count += 1
        ps.ExecuteWithProgress('Fetching discrepancies from %s ...' % data_source_name,
                               "Fetching discrepancies, batch number %d of %d" % (discrepancy_batch_count, number_of_batches),
                               fetch_data_from_eniq_async)


def get_formatted_query_components(cursors,tablelist, attribute_type, attribute_name_in_ENIQ_mapping):
    '''
    Transform parameters into valid values for SQL query
    '''
    mo_class = cursors[Rule.MOClass].CurrentValue.strip()
    identity = set_null_to_empty_string(cursors[Rule.ID].CurrentValue.strip())
    attribute = cursors[Rule.Attribute].CurrentValue.strip()
    vector_index = set_null_to_empty_string(cursors[Rule.VectorIndex].CurrentValue.strip())
    where_condition = set_null_to_empty_string(cursors[Rule.Where].CurrentValue.strip())
    wanted_value = cursors[Rule.Value].CurrentValue.strip()
    rule_name = cursors[Rule.RuleName].CurrentValue.strip()                          
    validation_status = cursors[Rule.ValidationStatus].CurrentValue.strip()                               
    tableName=cursors[Rule.TableName].CurrentValue.strip().upper()
    mo_tuple = (mo_class.lower(), attribute.lower())
    wanted_value_type = attribute_type[mo_tuple] if mo_tuple in attribute_type else 'String'
    attribute_name = attribute_name_in_ENIQ_mapping[mo_tuple] if mo_tuple in attribute_name_in_ENIQ_mapping else attribute
    finaltablename=tableName
    try:
        if (tableName == '' or tableName=='(EMPTY)') and len(list(tablelist[mo_tuple]))==1:
            finaltablename=list(tablelist[mo_tuple])[0]
        else:
            finaltablename=tableName
    except Exception as e:
        print("Exception: ", e)
    if wanted_value.lower() == 'null':
        attribute_str = "str(%s)" % attribute_name
        comparison_test_format = "({0} IS NOT {1})"
    elif wanted_value_type == 'Integer':
        attribute_str = "str(%s)" % attribute_name
        comparison_test_format = '({0} IS NULL OR {0}<>{1})'
    else:
        attribute_str = attribute_name
        if '=' in wanted_value:
            comparison_test_format = "({0} IS NULL OR {0} NOT LIKE '%%{1}')"
                                                
        elif wanted_value.lower() == 'null':
                                                 
            comparison_test_format = "({0} IS NOT {1})"
                                                    
        else:    
            comparison_test_format = "({0} IS NULL OR {0}<>'{1}')"
    
        
    return mo_class, attribute_name, finaltablename,identity, wanted_value, vector_index, where_condition, rule_name, validation_status, attribute_str, comparison_test_format


def get_sql_for_query(rules_table, bulk_cm_table_for_mo, selected_date, excluded_nodes, attribute_type, attribute_name_in_ENIQ_mapping):
    '''
    Build SQL queries to get discrepancies and MO counts
    '''
    queries = {}
    rules_rows = get_rules_rows(rules_table)
    columns = [Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName, Rule.ValidationStatus,Rule.TableName]
    cursors = {column: DataValueCursor.CreateFormatted(rules_table.Columns[column]) for column in columns}
    num_invalid_rules = 0
    num_valid_rules = 0
    for row in rules_table.GetRows(rules_rows, Array[DataValueCursor](cursors.values())):
                   
        mo_class, attribute,bulk_cm_table, identity, wanted_value, vector_index, where_condition, rule_name, validation_status, attribute_str, comparison_test_format = get_formatted_query_components(cursors,bulk_cm_table_for_mo, attribute_type,attribute_name_in_ENIQ_mapping)
        if validation_status == 'Valid': 
            if not bulk_cm_table:
                print('No Bulk CM table found for %s' % mo_class)
            else:
                rule_tuple = (mo_class, attribute, identity, wanted_value, vector_index, where_condition)
                                             
                sql = get_sql_for_discrepancy_query(rule_tuple, rule_name, attribute_str, excluded_nodes, bulk_cm_table, comparison_test_format, selected_date)
                num_valid_rules += 1
                queries[sql] = (rule_name, rule_tuple)
        else:
            num_invalid_rules += 1

    return queries


def get_sql_for_discrepancy_query(rule_tuple, rule_name, attribute_str, excluded_nodes, bulk_cm_table, comparison_test_format, selected_date):
    '''
    Build a SQL query to get discrepancies based on conditional arguments
    '''
    mo_class, attribute, identity, wanted_value, vector_index, where_condition = rule_tuple
    where_condition_clause = ''
    if where_condition:
        datetime_condition = "date_id='{}' and ".format(selected_date)
        where_condition_clause = ' and ' + re.sub(r'(from\s+(\w+)\s+where\s+)', r'\1\2.' + datetime_condition, where_condition, flags=re.IGNORECASE)

    where_clause = '{}{}{}{}'.format(
        "ELEMENT NOT IN {}".format(excluded_nodes),
        " and MOID like '%%{}={}'".format(mo_class, identity) if identity else '',
        " and DCVECTOR_INDEX={}".format(vector_index) if vector_index else '',
        " and {}_RAW.date_id='{}'".format(bulk_cm_table, selected_date)
    )

    rule_name = rule_name.replace("'", '"')
    rule_dict = {
        'rule_name': rule_name,
        'mo_class': mo_class,
        'identity' : identity,
        'attribute' : attribute,
        'vector_index': vector_index,
        'attribute_str': attribute_str,
        'bulk_cm_table': bulk_cm_table,
        'comparison_test_format': comparison_test_format.format(attribute, wanted_value),
        'where_clause': where_clause,
        'where_condition_clause': where_condition_clause
    }

    sql = """SELECT DATE_ID, ELEMENT, OSS_ID, '{rule_name}' as Rule, SN + ',' + MOID as FDN, '{mo_class}' as MOClass, '{identity}' as ID, '{attribute}' as Attribute, '{vector_index}' as VectorIndex, {attribute_str} as CurrentValue, (SELECT count(*) as RowCount FROM {bulk_cm_table}_RAW WHERE {where_clause}{where_condition_clause} and ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED')),(SELECT  count(*) as Discrepancies_Rowcount FROM {bulk_cm_table}_RAW WHERE {where_clause}{where_condition_clause} and ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED') and {comparison_test_format}) FROM {bulk_cm_table}_RAW WHERE {comparison_test_format} and ROWSTATUS NOT IN ('DUPLICATE','SUSPECTED') and {where_clause}{where_condition_clause}""".format(
        **rule_dict
    )

        
    return sql


def switch_to_results_page():
    '''
    Navigate to discrepancies pages when rules have been executed.
    '''
    for page in Document.Pages:
        if calculate_percentage_discrepancies and page.Title == discrepancies_table_name:
            Document.ActivePageReference = page
            break


def fetch_mo_attribute_details():
    '''
    Gets the CM Attribute details which will be used to validate rule inputs
    '''
    data_table = Document.Data.Tables['CM Attributes']
    rows = IndexSet(data_table.RowCount, True)
    columns = [Attribute.MOClass, Attribute.Attribute, Attribute.TableName, Attribute.ENIQDataType]
    cursors = {column: DataValueCursor.CreateFormatted(data_table.Columns[column]) for column in columns}
    records = []
    for _ in data_table.GetRows(rows, Array[DataValueCursor](cursors.values())):
        records.append(tuple([cursors[column].CurrentValue for column in columns]))

    attribute_type = {}
    attribute_name_in_ENIQ = {}
    bulk_cm_table_for_mo = {}
    mo_classes = set()
    for mo_class, attribute, bulk_cm_table, eniq_datatype in records:
        mo_classes.add(mo_class)
        datatype = 'String'
        if 'int' in eniq_datatype:
            datatype = 'Integer'
        mo_tuple = (mo_class.lower(), attribute.lower())
        if mo_tuple not in attribute_type:
            attribute_type[mo_tuple] = datatype
            attribute_name_in_ENIQ[mo_tuple] = attribute
            bulk_cm_table_for_mo[mo_tuple]=set()
            bulk_cm_table_for_mo[mo_tuple].add(bulk_cm_table)
        else:
            bulk_cm_table_for_mo[mo_tuple].add(bulk_cm_table)
    return bulk_cm_table_for_mo, attribute_type, attribute_name_in_ENIQ 
                                                 
def apply_replace_column_transformation():
    table = Document.Data.Tables['Discrepancies']
    for col in table.Columns:
        if col.Name == "DATE_ID" and col.DataType.ToString() == "DateTime":
            column_to_transform = DataColumnSignature(col)
            replace_transform = ReplaceColumnTransformation(column_to_transform,"DATE_ID","Date([DATE_ID])")
            table.AddTransformation(replace_transform)                           

            
Rule = Rule()
Attribute = Attribute()
Normal, Vector = range(2)
        
def main():
    try:
        clear_existing_discrepancies()
        bulk_cm_table_for_mo, attribute_type, attribute_name_in_ENIQ_mapping = fetch_mo_attribute_details()
        excluded_nodes = get_excluded_nodes(excluded_table_name)

        discrepancy_queries = get_sql_for_query(rules_table, bulk_cm_table_for_mo, selected_date, excluded_nodes, attribute_type, attribute_name_in_ENIQ_mapping)
        
        if len(discrepancy_queries) > 0:
            fetch_data_from_ENIQ(discrepancy_queries, data_source_name, discrepancies_table)
            switch_to_results_page()
            Document.Properties["RulesExcuted"] = "Excuted"
        else:
            notify.AddWarningNotification("Exception", "Error in executing rules", "No valid rules have been selected for execution.")
        apply_replace_column_transformation()
    except Exception as e:
        print "error",e   
        notify.AddWarningNotification("Exception", "Error in executing rules","Invalid Selection")
main()
    
    
