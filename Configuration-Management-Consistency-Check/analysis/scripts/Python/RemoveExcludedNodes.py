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
# Name    : RemoveExcludedNodes.py
# Date    : 12/09/2023
# Revision: 1.0
# Purpose : Delete marked excluded nodes from netan database
#
# Usage   : CMCC Analysis
#

import ast
import clr
clr.AddReference('System.Data')
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


def get_list_of_selected_nodes():
    """
    Generates list of user selected node ids from tblExcludedNodes

    Returns:
        selected_rules {list} -- list of rule names selected by the user
    """
    Document.Properties['SaveNodeError'] = ''

    excluded_nodes_table = Document.Data.Tables['tblExcludedNodes']
    data_filtering_selection = Document.Data.Filterings["Filtering scheme"]
    filtering_scheme = Document.FilteringSchemes[data_filtering_selection]
    filter_collection = filtering_scheme[excluded_nodes_table]
    filtered_rows = filter_collection.FilteredRows
    my_col_cursor = DataValueCursor.CreateFormatted(excluded_nodes_table.Columns["NodeID"])
    rows_to_remove=[]

    for row in excluded_nodes_table.GetRows(filtered_rows,my_col_cursor):
        node_id = my_col_cursor.CurrentValue
        rows_to_remove.Add(node_id)

    if len(rows_to_remove) == 0:
        Document.Properties['SaveNodeError'] = 'To remove nodes from exclusion list, select a node from the list on the right.'
    return rows_to_remove


def apply_parameters(command, query_parameters, column_list):
    """ 
    for an ODBC command, add all the required values for the parameters.
    """
    parameter_index = 0

    for col, col_value in query_parameters.items():
        for column_name, odbc_col_type in column_list.items():
            command.Parameters.Add(
                "@col"+str(parameter_index), odbc_col_type).Value = str(col_value[column_name])
            parameter_index += 1

    return command


def write_delete_from_db(sql, query_parameters, column_list):
    """
    Executes sql query in NetAn db to delete nodes from exclusion list

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


def create_value_list_for_sql(parameter_dict, column_list):
    """
      create a string in the format of (?,?,?)etc. so that the correct amount of command parameters can be added.
    """
    overall_rows = []
    for sql_column in parameter_dict.items():
        value_list = []
        current_row = ""

        for i in range(len(column_list)):
            value_list.append('?')

        current_row = """{0}""".format(','.join(value_list))
        overall_rows.append(current_row)
    return ','.join(overall_rows)


def delete_rows(marked_rows, data_table, id_column_name):
    """
    Deletes nodes selected by the user

    Returns:
        marked_rows {list} -- list of user selected rows
        data_table {DataTable} -- data table to remove rows from
    """
    parameter_list = {}
    parameter_index = 0
    columns_for_insert = {"NodeID": OdbcType.Int}
    cursor = DataValueCursor.CreateFormatted(
        data_table.Columns[id_column_name])
    rows_to_remove = IndexSet(data_table.RowCount, False)
    for row in data_table.GetRows(cursor):
        row_id = cursor.CurrentValue

        if row_id in marked_rows:
            rows_to_remove.AddIndex(row.Index)
            parameter_list[parameter_index] = {"NodeID": row_id}
            sql = 'Delete from "tblExcludedNodes" where "NodeID" IN ( '
            sql += create_value_list_for_sql(parameter_list,
                                                columns_for_insert)
            sql += ' )'
            write_delete_from_db(sql, parameter_list, columns_for_insert)
            parameter_list.clear()


def main():
    """
    main flow
    """
    excluded_nodes_table = Document.Data.Tables['tblExcludedNodes']
    marked_rows = get_list_of_selected_nodes()
    id_column_name = 'NodeID'
    delete_rows(marked_rows, excluded_nodes_table, id_column_name)
    excluded_nodes_table.Refresh()


main()