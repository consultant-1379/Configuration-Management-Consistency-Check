# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2021 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : NetAnConnection.py
# Date    : 15/04/2021
# Revision: 5.0
# Purpose : 
#
# Usage   : PM Explorer
#

import clr
clr.AddReference('System.Data')
from collections import OrderedDict
from System.Data.Odbc import OdbcConnection
import ast
from System import Array, Byte
from System.Text import UTF8Encoding
from System.IO import MemoryStream
from System.Security.Cryptography import RijndaelManaged, CryptoStream, CryptoStreamMode

netan_db = Document.Properties["NetAnDB"]
netan_username = Document.Properties["NetAnUserName"]
netan_password = Document.Properties["NetAnPassword"]
database_name = "netAnServer_pmdb"
MSSQL_DRIVER_LIST = [
    "{SQL Server}",
    "{ODBC Driver 13 for SQL Server}",
    "{ODBC Driver 17 for SQL Server}",    
    "{ODBC Driver 11 for SQL Server}" 
]

_key    = ast.literal_eval(Document.Properties['valArray'])
_vector = [0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0]

_key = Array[Byte](_key)
_vector = Array[Byte](_vector)


def _to_bytes(lst):
    return ''.join(map(chr, lst))


def _to_hex_digest(encrypted):
    return ''.join(map(lambda x: '%02x' % x, encrypted))


def encrypt(text, digest=True):
    '''
    Performs crypting of provided text using AES algorithm.
    If 'digest' is True hex_digest will be returned, otherwise bytes of encrypted
    data will be returned.
    
    This function is simetrical with decrypt function.
    '''
    utf_encoder    = UTF8Encoding()
    bytes         = utf_encoder.GetBytes(text)
    rm            = RijndaelManaged()
    enc_transform = rm.CreateEncryptor(_key, _vector)
    mem           = MemoryStream()
    
    cs = CryptoStream(mem, enc_transform, CryptoStreamMode.Write)
    cs.Write(bytes, 0, len(bytes))
    cs.FlushFinalBlock()
    mem.Position = 0
    encrypted = Array.CreateInstance(Byte, mem.Length)
    mem.Read(encrypted, 0, mem.Length)
    cs.Close()
        
    l = map(int, encrypted)
    return _to_hex_digest(l) if digest else _to_bytes(l)


def validate_empty_fields(od):
    """
    checking for values if it contains any symbols which is not accepted by ENM
    """
    response = ''
    if len(od) != 0:
        for key, value in od.items():
            if value != None: 
                if not value.strip():
                    response = key
                    break
            else:
                response = "Required Field cannot be None"
    return response


def validate_errors():
    is_valid = True
    errorMessage = ""     
    dp = OrderedDict()
    dp["NetAn SQL DB URL"] = netan_db 
    dp["NetAn User Name"] = netan_username
    dp["NetAn Password "] = netan_password
    empty_fields = validate_empty_fields(dp)
    if len(empty_fields)>0 :
        is_valid = False
    error_message = "Provide value for: " + str(empty_fields)
    Document.Properties["NetAnResponseCode"] = error_message
    return is_valid


def get_connection_string_driver():
    try:
        conn_string = "Driver={PostgreSQL Unicode(x64)};Server="+netan_db+";Port=5432;Database="+database_name+";Uid="+netan_username+";Pwd="+netan_password+";" 
        test_netan_db_conn(conn_string)
        return "Driver={PostgreSQL Unicode(x64)};Server="+netan_db+";Port=5432;Database="+database_name+";Uid="+netan_username+";Pwd=@NetAnPassword;" 
    except:
        for driver_name in MSSQL_DRIVER_LIST:
            try:
                test_connection_string = """Driver={driver_name};Server={netan_db};Database={database_name};UID={netan_username};PWD={netan_password};"""
                conn_string = test_connection_string.format(driver_name=driver_name, NetAnDB=netan_db,database_name=database_name,netan_username=netan_username,netan_password=netan_password)
                test_netan_db_conn(conn_string)
                print ("Connected using driver: ", driver_name)
                return test_connection_string.format(driver_name=driver_name, netan_db=netan_db,database_name=database_name,netan_username=netan_username,netan_password="@NetAnPassword")

            except Exception as e:
                print("Error connecting with driver: " + driver_name + " Testing next driver...")           
                if "Login" in str(e.message):
                    print e.message
                    Document.Properties["NetAnResponseCode"] = "Login Failed" 
                else:
                    Document.Properties["NetAnResponseCode"] = "Cannot Create Connection"


def test_netan_db_conn(conn_string):
    error_msg = ""
    sql = "SELECT count(*) from \"tblAlarmDefinitions\""
    connection = OdbcConnection(conn_string)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    reader = command.ExecuteReader();
    loopguard = 0
    while reader.Read() and loopguard != 1:
        error_msg = reader[0]
        loopguard = 1
    connection.Close()
    Document.Properties["NetAnResponseCode"] = "Connection OK"
    

def check_eniq_ds_table(conn_string):
    '''Checks how many eniqs connected '''
    sql = "SELECT count(*) from \"tblEniqDS\""
    connection = OdbcConnection(conn_string)
    connection.Open()
    command = connection.CreateCommand()
    command.CommandText = sql
    reader = command.ExecuteReader();
    loopguard = 0
    connected_eniqs = 0
    while reader.Read() and loopguard != 1:
        connected_eniqs = reader[0]
        loopguard = 1
    connection.Close()
    return connected_eniqs


def add_to_eniq_ds_table(conn_string):
    '''Inserts new eniq into the Eniq_DS table'''
    eniq_name = "NetAn_ODBC"
    sql = """INSERT into "tblEniqDS" ("EniqName") Values ('{}') 
           """.format(eniq_name)
    return (write_to_DB(sql, conn_string))


def migrate_collections(conn_string):
    '''Adds an Eniq datasource to any already existing collections that don't have one '''
    eniq_name = "NetAn_ODBC"
    sql_query = '''
    UPDATE "tblCollection"
    SET "EniqID"  = (select "EniqID" from "tblEniqDS" where "EniqName" = '{0}') 
    WHERE "EniqID" is null;
    '''.format(eniq_name)
    write_to_DB(sql_query, conn_string) 


def write_to_DB(sql, conn_string):
    try:
        connection = OdbcConnection(conn_string)
        connection.Open()
        command = connection.CreateCommand()
        command.CommandText = sql
        command.ExecuteReader()
        connection.Close()
        return True
    except Exception as e:
        Document.Properties['ConnectionError'] = "** Error when saving collection"
        return False


if validate_errors(): 
    connection_string = get_connection_string_driver()
    if connection_string is not None:
        connection_string1 = connection_string.replace("@NetAnPassword", Document.Properties["NetAnPassword"])
        if check_eniq_ds_table(connection_string1) == 0: 
            add_to_eniq_ds_table(connection_string1)
            migrate_collections(connection_string1)
        Document.Properties["ConnStringNetAnDB"]= connection_string
        encrypt_netan = encrypt(Document.Properties["NetAnPassword"])
        Document.Properties["NetAnPassword"] = encrypt_netan


