import clr
clr.AddReference('System.Data')
from collections import OrderedDict

from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
from Spotfire.Dxp.Data import DataValueCursor

from System.Data.Odbc import OdbcConnection, OdbcType
from System.IO import MemoryStream
from System import Array, Byte
from System.Text import UTF8Encoding
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode

import ast

#Input Parameters for rules

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



def check_for_duplicate_node(node_name_input, data_table):

    result = ''
    node_list = []
    duplicate_found = False
    cursor = DataValueCursor.CreateFormatted( data_table.Columns['NodeName'])

    for row in data_table.GetRows(cursor):
        index = row.Index
        node_name = data_table.Columns['NodeName'].RowValues.GetFormattedValue(index)

        node_list.append(node_name)

    if node_name_input in node_list:
        result = 'Node is already excluded'
        duplicate_found = True
    
    Document.Properties['SaveNodeError'] = result
    return duplicate_found


def save_node_to_db():
    excluded_node = Document.Properties['ExcludedNodeInput'].strip()
    excluded_node_list_table = Document.Data.Tables['tblExcludedNodes']
    node_name_column = 'NodeName'
    odbc_type = OdbcType.VarChar

    sql_query = '''
                INSERT INTO "tblExcludedNodes" ("{node_name_col}") VALUES (?);
                '''.format(node_name_col = node_name_column)

    duplicate_node_found = check_for_duplicate_node(excluded_node, excluded_node_list_table)

    if not duplicate_node_found and excluded_node!= '':
        write_to_db(sql_query, excluded_node, odbc_type)

    excluded_node_list_table.Refresh()


def write_to_db(sql, excluded_node, odbc_type):
    """
    Executes sql query in NetAn db to insert/delete reports
    """
    conn_string = Document.Properties['ConnStringNetAnDB'].replace(
        "@NetAnPassword", decrypt(Document.Properties['NetAnPassword']))

    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command.Parameters.Add(
                "@col"+str(0), odbc_type).Value = excluded_node
        command.ExecuteNonQuery()
        connection.Close()
        return True
    except Exception as e:
        print("Error: " + str(e.message))
        return False
#Input Parameters for rules


save_node_to_db()


