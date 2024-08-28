from System.Collections.Generic import Dictionary, List
from Spotfire.Dxp.Data import DataValueCursor, IndexSet, DataColumnSignature, AddRowsSettings, AddColumnsSettings, JoinType, HierarchyDefinition, HierarchyNestingMode, DataProperty, DataType, DataPropertyClass, RowSelection, DataMarkingSelection, DataSelectionOperation
from Spotfire.Dxp.Data.Import import DatabaseDataSource, DatabaseDataSourceSettings, DataTableDataSource, TextDataReaderSettings, TextFileDataSource
from Spotfire.Dxp.Framework.ApplicationModel import *
from System.IO import MemoryStream, StreamWriter, SeekOrigin
from System import Array
import re
import time
import sys


startTime = time.time()

"""
Spotfire Properties:
"""
batchSize = Document.Properties["BatchSize"]
selectedDate = Document.Properties["SelectedDate"].ToString('yyy-MM-dd HH:mm:ss')  # Fetch selected date from drop-down filter
dataSourceName = Document.Properties["DataSourceName"]
calculatePercentageDiscrepancies = Document.Properties["CalculatePercentageDiscrepancies"]
print("Date = %s, DataSourceName = %s, BatchSize = %s, CalculatePercentageDiscrepancies = %s" % (selectedDate, dataSourceName, batchSize, calculatePercentageDiscrepancies))
queryResult = "QueryResult"  # save result of query in this document property
databaseConnectionResult = "DatabaseConnectionResult"
rulesMarkingName = 'MarkingRules'

"""
Scheduling, fetch Platform Info
"""
from Spotfire.Dxp.Framework.ApplicationModel import NotificationService
applicationType =Application.GetType().ToString()
analysisWebClient = 'Spotfire.Dxp.Web.WebAnalysisApplication'
print (applicationType)
# Document.Properties["applicationType"] = analysisWebClient

rulesTableName            = 'Rules'           # input table
blacklistTableName        = 'NodeBlacklist'   # input table
mappingTableName          = 'CM Attributes'   # input table
moCountsTableName         = 'MOCounts'        # output table
discrepanciesTableName    = 'Discrepancies'   # output table
invalidRulesTableName     = 'Invalid Rules'
validationStatusTableName = 'Validation Status'

invalidCauseDescriptionColumnName = 'Invalid Cause Description'

rulesTable         = Document.Data.Tables[rulesTableName]
discrepanciesTable = Document.Data.Tables[discrepanciesTableName]
moCountsTable      = Document.Data.Tables[moCountsTableName]


class Rule():
    MOClass = 'MOClass'
    Attribute = 'Attribute'
    ID = 'ID'
    Value = 'Value'
    VectorIndex = 'VectorIndex'
    Where = 'Where'
    RuleName = 'RuleName'
    WhereCondition = 'WhereCondition'


class Attribute():
    SystemArea = 'SystemArea'
    NodeType = 'NodeType'
    MOClass = 'MOClass'
    Attribute = 'Attribute'
    Min = 'Min'
    Max = 'Max'
    DefaultValue = 'DefaultValue'
    Unit = 'Unit'
    Description = 'Description'
    MultiplicationFactor = 'MultiplicationFactor'
    Resolution = 'Resolution'
    MinLength = 'MinLength'
    MaxLength = 'MaxLength'
    UndefinedValue = 'UndefinedValue'
    Boolean = 'Boolean'
    LengthRange = 'LengthRange'
    Long = 'Long'
    Longlong = 'Longlong'
    Range = 'Range'
    SeqDefaultValue = 'SeqDefaultValue'
    Sequence = 'Sequence'
    String = 'String'
    EnumRef = 'EnumRef'
    MoRef = 'MoRef'
    StructRef = 'StructRef'
    Condition = 'Condition'
    Dependencies = 'Dependencies'
    Deprecated = 'Deprecated'
    Disturbances = 'Disturbances'
    LockBeforeModify = 'LockBeforeModify'
    Obsolete = 'Obsolete'
    Precondition = 'Precondition'
    ReadOnly = 'ReadOnly'
    SamplingRate = 'SamplingRate'
    Specification = 'Specification'
    TakesEffect = 'TakesEffect'
    TableName = 'TableName'
    StructRef = 'StructRef'
    TableKeys = 'TableKeys'
    Element = 'Elemen'


# Fetch blacklisted nodes to be excluded from results
def getBlacklistedNodes(blacklistTableName):
    blacklist = []
    blacklistTable = Document.Data.Tables[blacklistTableName]
    cursorNodeName = DataValueCursor.CreateFormatted(blacklistTable.Columns["NodeName"])
    blacklistRows = IndexSet(blacklistTable.RowCount, True)
    for node in blacklistTable.GetRows(blacklistRows, cursorNodeName):
        blacklist.append(cursorNodeName.CurrentValue)
    #print('Blacklist Nodes: ', blacklist)
    return "('%s')" % "','".join(blacklist)


# This function splits a list into batches so that resulting queries don't exceed IQ SQL limits, e.g. can't have more than 512 tables in a single query (SQL Anywhere Error -1001030: Feature, More than 512 tables in a query, is not supported.)
def divideIntoBatches(list_name, n):
    for i in range(0, len(list_name), n):
        yield list_name[i:i+n]

# Get marked rules, else return all rules
def getRulesRows(rulesTable):
    # Execute Marked rules only using default Marking scheme
    rulesRows = Document.Data.Markings[rulesMarkingName].GetSelection(rulesTable).AsIndexSet()
    print('Number of marked rules = %d' % rulesRows.Count)
    # if there are no marked rules, execute for all rules
    if rulesRows.Count == 0:
        rulesRows = IndexSet(rulesTable.RowCount, True)
        print('Total number of rules = %s' % rulesRows.Count)
    return rulesRows


def setNullToEmptyString(stringToEmpty):
    return stringToEmpty if stringToEmpty != '(Empty)' else ''


def splitList(aList):
  half = len(aList) // 2
  return aList[:half], aList[half:]


# Fetch the MO counts from ENIQ
def fetchDataFromENIQ(discrepancyQueries, dataSourceName, dataTableName, progressText, replaceTableOnFirstQuery):
    global sql, queriesAwaitingExecution, queriesToTry
    sqlQueries = sorted(discrepancyQueries.keys())
    # print ("sqlQueries", sqlQueries)
    dataTable = Document.Data.Tables[dataTableName]
    dataTableAddRowsSettings = AddRowsSettings(dataTable, DataTableDataSource(dataTable))
    # if applicationType == analysisClient:
    #     ps = Application.GetService[ProgressService]()  # This is used to fetch data from ENIQ asynchronously
    queriesAwaitingExecution = []

    # This callback function cannot take parameters, so variables used are globals
    def fetchCountsFromENIQAsync():
        global sql, queriesAwaitingExecution, queriesToTry
        try:
            # print('SQL = %s' % sql)
            # if applicationType == analysisClient:
            #     ps.CurrentProgress.ExecuteSubtask(progressText)
            dataTableSettings = DatabaseDataSourceSettings("System.Data.Odbc", "DSN=" + dataSourceName, sql)
            print('replaceTableOnFirstQuery: ', replaceTableOnFirstQuery)
            print('sql for fetchDataFromENIQ', sql)
            dataTableDataSource = DatabaseDataSource(dataTableSettings)
            # if applicationType == analysisClient:
            #     ps.CurrentProgress.CheckCancel()

            if Document.Data.Tables.Contains(dataTableName):  # If exists, replace it
                if True in replaceTableOnFirstQuery:  # Dump old contents of table on running of first rule
                    print('Replace old table')
                    dataTable.ReplaceData(dataTableDataSource)
                    replaceTableOnFirstQuery.pop()  # empty this to indicate that the first query has been successful
                else:
                    print('Add rows to existing table')
                    dataTable.AddRows(dataTableDataSource, dataTableAddRowsSettings)
            else:  # If it does not exist, create new
                print('Create new table')
                Document.Data.Tables.Add(dataTableName, dataTableDataSource)
            Document.Properties[queryResult] = 'OK'
            print("Successful queriesAwaitingExecution before: ", queriesAwaitingExecution)
            print("Successful queriesToTry before: ", queriesToTry)
            queriesToTry = [i for i in queriesAwaitingExecution if i not in queriesToTry]  # remove the list with successful rule(s), and reset the next list to try to the remaining rules
            queriesAwaitingExecution = []
            print("Successful queriesAwaitingExecution after: ", queriesAwaitingExecution)
            print("Successful queriesToTry after: ", queriesToTry)
        except ProgressCanceledException as pce:  # user cancelled
            print("ProgressCanceledException: ", pce)
            Document.Properties[queryResult] = 'User cancelled'
        except Exception as e:
            print("Exception in fetching data from ENIQ: ", e)
            Document.Properties[queryResult] = 'Failed'
            print("queriesAwaitingExecution before: ", queriesAwaitingExecution)
            print("queriesToTry before: ", queriesToTry)
            queriesAwaitingExecution = [i for i in queriesAwaitingExecution if i not in queriesToTry]  # remove the list with failed rule(s)
            queriesRemaining, queriesToTry = splitList(queriesToTry)  # split into 2
            queriesAwaitingExecution.extend(queriesRemaining)  # add back in half the rules to be retried
            print("queriesAwaitingExecution after: ", queriesAwaitingExecution)
            print("queriesToTry after: ", queriesToTry)

    print('Number of  queries = %d' % len(sqlQueries))
    dataBatchSize = batchSize
    batchQueries = divideIntoBatches(sqlQueries, dataBatchSize)
    numberOfBatches = int(len(sqlQueries) / dataBatchSize) + 1
    batchCount = 0
    for batchQuery in batchQueries:
        print('batchQuery: ', batchQuery)
        queriesToTry = batchQuery[:]  # deep copy
        queriesAwaitingExecution = batchQuery[:]  # deep copy
        sql = ' union all '.join(queriesToTry)
        print('Number of SQL queries in this batch = %d' % len(queriesToTry))
        batchCount += 1
        # if applicationType == analysisClient:
        #     ps.ExecuteWithProgress(progressText, "Batch number %d of %d" % (batchCount, numberOfBatches), fetchCountsFromENIQAsync)
        # else:
        if applicationType == analysisWebClient:
            fetchCountsFromENIQAsync()
        while queriesToTry or queriesAwaitingExecution:
            if len(queriesToTry) == 1:
                print('Found an invalid rule:', queriesToTry)
                invalidSQL = queriesToTry[0]
                print('Invalid rule details:', discrepancyQueries[invalidSQL])
                invalidRuleName, (moClass, attribute, identity, wantedValue, vectorIndex, whereCondition) = discrepancyQueries[invalidSQL]
                print('Invalid rule details 2:', invalidRuleName, moClass, attribute, identity, wantedValue, vectorIndex, whereCondition, invalidSQL)
                invalidRulesWithCause.add((moClass, attribute, identity, wantedValue, vectorIndex, whereCondition, invalidRuleName, invalidSQL))
                # invalidRules.add(extractRuleNameFromQuery(queriesToTry[0]))  # extract RuleName from query
                # print('invalidRules in while: ', invalidRules)
                queriesToTry = queriesAwaitingExecution[:]  # deep copy, need to reset this to try the next list of rules
                sql = ' union all '.join(queriesToTry)
                # if applicationType == analysisClient:
                #     ps.ExecuteWithProgress(progressText, "Found a invalid rule, retrying batch number %d of %d" % (batchCount, numberOfBatches), fetchCountsFromENIQAsync)
                # else:
                if applicationType == analysisWebClient:
                    fetchCountsFromENIQAsync()
            else:
                sql = ' union all '.join(queriesToTry)
                # if applicationType == analysisClient:
                #     ps.ExecuteWithProgress(progressText, "Retrying batch number %d of %d" % (batchCount, numberOfBatches), fetchCountsFromENIQAsync)
                # else:
                if applicationType == analysisWebClient:
                    fetchCountsFromENIQAsync()


def extractRuleNameFromQuery(query):
    return unescapeQuoteInRuleName(re.search(r'\'(.*)\' as Rule', query).group(1))


# Transform parameters into valid values for SQL query
def getFormattedQueryComponents(cursors):
    moClass = cursors[Rule.MOClass].CurrentValue.strip()
    identity = setNullToEmptyString(cursors[Rule.ID].CurrentValue.strip())
    attribute = cursors[Rule.Attribute].CurrentValue.strip()
    vectorIndex = setNullToEmptyString(cursors[Rule.VectorIndex].CurrentValue.strip())
    whereCondition = setNullToEmptyString(cursors[Rule.Where].CurrentValue.strip())
    wantedValue = cursors[Rule.Value].CurrentValue.strip()
    ruleName = cursors[Rule.RuleName].CurrentValue.strip()
    comparisonTestFormat = '(%s<>%s)'  # default test for simple "attr<>1" type checks

    # Need to do some data type conversions for the SQL query to be valid, e.g. attribute must be a string in RuleName
    # Also need to match the comparison operator to the required WantedValue type
    try:  # cast from string to Integer to determine data type of wantedValue
        wantedValue = int(wantedValue)
        attributeStr = "str(%s)" % attribute
    except:  # must be non-integer
        attributeStr = attribute
        if '=' in wantedValue:  # wantedValue is something like "PreschedProfile=0" so need to use NOT LIKE instead of <> to test match
            comparisonTestFormat = "(%s not like '%%%s')"
        else:
            comparisonTestFormat = "(%s<>'%s')"

    return moClass, attribute, identity, wantedValue, vectorIndex, whereCondition, ruleName, attributeStr, comparisonTestFormat


# columns = [Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName, invalidCauseDescriptionColumnName]
# cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in columns}
# for row in dataTable.GetRows(rows, Array[DataValueCursor](cursors.values())):
#     invalidRules.add(tuple([cursors[column].CurrentValue if cursors[column].CurrentValue != '(Empty)' else '' for column in columns]))
#     invalidRuleNames.add(cursors[Rule.RuleName].CurrentValue)


# Build SQL queries to get discrepancies and MO counts
def getSQLForQuery(rulesTable, bulkCmTableForMO, selectedDate, blacklistNodes):
    queries = {}
    rulesRows = getRulesRows(rulesTable)
    columns = [Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName]
    cursors = {column: DataValueCursor.CreateFormatted(rulesTable.Columns[column]) for column in columns}
    for row in rulesTable.GetRows(rulesRows, Array[DataValueCursor](cursors.values())):
        moClass, attribute, identity, wantedValue, vectorIndex, whereCondition, ruleName, attributeStr, comparisonTestFormat = getFormattedQueryComponents(cursors)
        bulkCmTable = ''
        print("Rule is %s" % ruleName)
        if vectorIndex and (moClass, Vector) in bulkCmTableForMO:
            bulkCmTable = bulkCmTableForMO[(moClass, Vector)]
        elif (moClass, Normal) in bulkCmTableForMO:
            bulkCmTable = bulkCmTableForMO[(moClass, Normal)]
        if not bulkCmTable:
            print('No Bulk CM table found for %s' % moClass)
        else:
            ruleTuple = (moClass, attribute, identity, wantedValue, vectorIndex, whereCondition)
            sql = getSQLForDiscrepancyQuery(ruleTuple, ruleName, attributeStr, blacklistNodes, bulkCmTable, comparisonTestFormat, selectedDate)
            queries[sql] = (ruleName, ruleTuple)
    return queries


# Build a SQL query to get discrepancies based on conditional arguments
def getSQLForDiscrepancyQuery(ruleTuple, ruleName, attributeStr, blacklistNodes, bulkCmTable, comparisonTestFormat, selectedDate):
    moClass, attribute, identity, wantedValue, vectorIndex, whereCondition = ruleTuple


    '''  Sample SQL query
SELECT DATETIME_ID, ELEMENT, OSS_ID, 'AdmissionControl - dlAdmDifferentiationThr should be 800' as Rule, SN + ',' + MOID as FDN, 'AdmissionControl' as MOClass, '' as ID,
'dlAdmDifferentiationThr' as Attribute, '' as VectorIndex, str(dlAdmDifferentiationThr) as CurrentValue,
(
SELECT count(MOID) as MOCount
FROM DC_E_BULK_CM_ADMISSIONCONTROL_RAW
WHERE datetime_id='2020-09-30 03:00:00'
and ELEMENT NOT IN ('9C0704','KOFA02','OLMP04','OLMP05','OLMP08','OLMP10','OLMP11','OLMP14','OLMP15','OLMP16','OLMP18','OLMP20','WSWC06')
)
FROM DC_E_BULK_CM_ADMISSIONCONTROL_RAW
WHERE (dlAdmDifferentiationThr<>800) and datetime_id='2020-09-30 03:00:00'
and ELEMENT NOT IN ('9C0704','KOFA02','OLMP04','OLMP05','OLMP08','OLMP10','OLMP11','OLMP14','OLMP15','OLMP16','OLMP18','OLMP20','WSWC06')
    '''

    sqlTemplate = """
SELECT DATETIME_ID, ELEMENT, OSS_ID, '{0}' as Rule, SN + ',' + MOID as FDN, '{1}' as MOClass, '{2}' as ID, '{3}' as Attribute, '{4}' as VectorIndex, {5} as CurrentValue,
(
 SELECT count(MOID) as MOCount
 FROM {6}_RAW
 WHERE {8}
)
FROM {6}_RAW WHERE {7} and {8}{9}"""

    whereClause = '{}{}{}{}'.format(
        "ELEMENT NOT IN {}".format(blacklistNodes),
        " and MOID like '%%{}={}'".format(moClass, identity) if identity else '',
        " and DCVECTOR_INDEX={}".format(vectorIndex) if vectorIndex else '',
        " and {}_RAW.datetime_id='{}'".format(bulkCmTable, selectedDate)
    )
    print('whereClause: ', whereClause)
    whereConditionClause = ''
    if whereCondition:
        print('whereCondition: ', whereCondition)
        datetimeCondition = "datetime_id='{}' and ".format(selectedDate)
        print('datetimeCondition: ', datetimeCondition)
        whereConditionClause = ' and ' + re.sub(r'(from\s+(\w+)\s+where\s+)', r'\1\2.' + datetimeCondition, whereCondition, flags=re.IGNORECASE)  # add in datetime condition for relevant tables in any custom Where condition
    print('whereConditionClause: ', whereConditionClause)
    sql = sqlTemplate.format(
        ruleName,
        moClass,
        identity,
        attribute,
        vectorIndex,
        attributeStr,
        bulkCmTable,
        comparisonTestFormat % (attribute, wantedValue),
        whereClause,
        whereConditionClause
    )
    print('Discrepancies queries SQL is ' + sql)
    return sql


def switchToResultsPage():
    # Switch to results page
    for page in Document.Pages:
        if calculatePercentageDiscrepancies and page.Title == discrepanciesTableName + ' (Statistics)':
            Document.ActivePageReference = page
            break
        elif page.Title == discrepanciesTableName:
            Document.ActivePageReference = page
            break


def fetchMOAttributeDetails():
    dataTable = Document.Data.Tables['CM Attributes']
    rows = IndexSet(dataTable.RowCount, True)
    columns = [Attribute.MOClass, Attribute.Attribute, Attribute.TableName, Attribute.Boolean, Attribute.Long, Attribute.Longlong]
    cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in columns}
    records = []
    for row in dataTable.GetRows(rows, Array[DataValueCursor](cursors.values())):
        records.append(tuple([cursors[column].CurrentValue for column in columns]))
    # print(records)
    attributeTable = {}
    attributeType = {}
    bulkCmTableForMO = {}
    moClasses = set()
    for moClass, attribute, bulkCmTable, booleanType, longType, longlongType in records:
        moClasses.add(moClass)
        if bulkCmTable.endswith('_V'):
            bulkCmTableForMO[(moClass, Vector)] = bulkCmTable
        else:
            bulkCmTableForMO[(moClass, Normal)] = bulkCmTable
        # bulkCmTableForMO[moClass] = bulkCmTable[:-2] if bulkCmTable.endswith('_V') else bulkCmTable  # ensure that table doesn't end with '_V
        # print(' {}, {}, {}'.format(booleanType, longType, longlongType))
        dataType = 'String'
        if booleanType == 'True':
            dataType = 'Boolean'
        elif longType == 'True' or longlongType == 'True':
            dataType = 'Integer'
        # print('{}, {}, {}'.format(moClass, attribute, dataType))
        moTuple = (moClass.lower(), attribute.lower())
        if moTuple not in attributeType:
            attributeType[moTuple] = dataType
            attributeTable[moTuple] = bulkCmTable
    # print(attributeTable)
    return bulkCmTableForMO, attributeType, attributeTable, moClasses


def unescapeQuoteInRuleName(ruleName):
    return ruleName.replace("''", "'")

#
# def cleanInvalidRules(invalidRules):
#     ruleNameCursor = DataValueCursor.CreateFormatted(rulesTable.Columns[Rule.RuleName])
#     rowsToRemoveFromRules = IndexSet(rulesTable.RowCount, False)
#     for row in rulesTable.GetRows(ruleNameCursor):
#         ruleNameQuoted = ruleNameCursor.CurrentValue
#         ruleNameUnquoted = unescapeQuoteInRuleName(ruleNameCursor.CurrentValue)
#         # print("Rule in table: ", ruleNameQuoted)
#         # print("Rule in table unquoted: ", ruleNameUnquoted)
#         if ruleNameQuoted in invalidRules or ruleNameUnquoted in invalidRules:
#             print("Found invalid rule in table: ", ruleNameUnquoted)
#             rowsToRemoveFromRules.AddIndex(row.Index)
#     print("rowsToRemove: ", rowsToRemoveFromRules)
#     print("rowsToKeep: ", rowsToRemoveFromRules.Not())
#     print("Excluded rowsToRemove count: ", RowSelection(rowsToRemoveFromRules).ExcludedRowCount)
#     print("Included rowsToRemove count: ", RowSelection(rowsToRemoveFromRules).IncludedRowCount)
#     print("rowsToRemove total count: ", RowSelection(rowsToRemoveFromRules).TotalRowCount)
#     rowsToRemoveCount = RowSelection(rowsToRemoveFromRules).ExcludedRowCount
#     if rowsToRemoveCount:  # Found some Invalid Rules, so do some cleaning
#         dataTableDataSource = DataTableDataSource(rulesTable)
#
#         # Copy all the rules to New Invalid Rules, and then remove all the good rules
#         newInvalidRulesTableName = 'New Invalid Rules'
#         if Document.Data.Tables.Contains(newInvalidRulesTableName):  # If exists, add rows to it
#             newInvalidRulesTable = Document.Data.Tables[newInvalidRulesTableName]
#             newInvalidRulesTable.ReplaceData(dataTableDataSource)
#             print('Add rows to New Invalid Rules table')
#         else:  # If it does not exist, create new
#             Document.Data.Tables.Add(newInvalidRulesTableName, dataTableDataSource)
#             newInvalidRulesTable = Document.Data.Tables[newInvalidRulesTableName]
#             print('Create New Invalid Rules table')
#         newInvalidRulesTable.RemoveRows(RowSelection(rowsToRemoveFromRules))  # remove all the good rules
#
#         # Append any New Invalid Rules to any existing Invalid Rules table
#         dataTableDataSource = DataTableDataSource(newInvalidRulesTable)
#         if Document.Data.Tables.Contains(invalidRulesTableName):  # If exists, add rows to it
#             invalidRulesTable = Document.Data.Tables[invalidRulesTableName]
#             invalidRulesTableAddRowsSettings = AddRowsSettings(invalidRulesTable, dataTableDataSource)
#             invalidRulesTable.AddRows(dataTableDataSource, invalidRulesTableAddRowsSettings)
#             print('Add rows to Invalid Rules table')
#         else:  # If it does not exist, create new
#             Document.Data.Tables.Add(invalidRulesTableName, dataTableDataSource)
#             print('New Invalid Rules table')
#
#         Document.Data.Tables.Remove(newInvalidRulesTableName)
#         rulesTable.RemoveRows(RowSelection(rowsToRemoveFromRules.Not()))  # remove the invalid rules from the Rules table so they will not be processed again


# def fetchExistingInvalidRules():
#     invalidRules = set()
#     if Document.Data.Tables.Contains(invalidRulesTableName):  # If exists, add rows to it
#         dataTable = Document.Data.Tables[invalidRulesTableName]
#         rows = IndexSet(dataTable.RowCount, True)
#         columns = [Rule.RuleName]
#         cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in columns}
#         for row in dataTable.GetRows(rows, Array[DataValueCursor](cursors.values())):
#             invalidRules.add(cursors[Rule.RuleName].CurrentValue)
#         print('Existing Invalid Rules: ', invalidRules)
#     return invalidRules


def cleanInvalidRules(invalidRules):
    ruleNameCursor = DataValueCursor.CreateFormatted(rulesTable.Columns[Rule.RuleName])
    rowsToRemoveFromRules = IndexSet(rulesTable.RowCount, False)
    invalidRuleNames = [x[6] for x in invalidRules]
    for row in rulesTable.GetRows(ruleNameCursor):
        ruleNameQuoted = ruleNameCursor.CurrentValue
        ruleNameUnquoted = unescapeQuoteInRuleName(ruleNameCursor.CurrentValue)
        # print("Rule in table: ", ruleNameQuoted)
        # print("Rule in table unquoted: ", ruleNameUnquoted)
        if ruleNameQuoted in invalidRuleNames or ruleNameUnquoted in invalidRuleNames:
            print("Found existing invalid rule in table: ", ruleNameUnquoted)
            rowsToRemoveFromRules.AddIndex(row.Index)
    print("rowsToRemove: ", rowsToRemoveFromRules)
    print("rowsToKeep: ", rowsToRemoveFromRules.Not())
    print("Excluded rowsToRemove count: ", RowSelection(rowsToRemoveFromRules).ExcludedRowCount)
    print("Included rowsToRemove count: ", RowSelection(rowsToRemoveFromRules).IncludedRowCount)
    print("rowsToRemove total count: ", RowSelection(rowsToRemoveFromRules).TotalRowCount)
    rowsToRemoveCount = RowSelection(rowsToRemoveFromRules).ExcludedRowCount
    if rowsToRemoveCount:  # Found some Invalid Rules, so do some cleaning
        rulesTable.RemoveRows(RowSelection(rowsToRemoveFromRules.Not()))  # remove the invalid rules from the Rules table so they will not be processed again


def validateRules(rulesTable, attributeType, attributeTable, moClasses):
    dataTable = rulesTable
    invalidRulesWithCause = set()
    rulesRows = getRulesRows(rulesTable)
    # rows = IndexSet(dataTable.RowCount, True)  # handle all rows
    columns = [Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName]
    cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in columns}
    for row in dataTable.GetRows(rulesRows, Array[DataValueCursor](cursors.values())):
        moClass = cursors[Rule.MOClass].CurrentValue.strip()
        # identity = setNullToEmptyString(cursors[Rule.ID].CurrentValue.strip())
        attribute = cursors[Rule.Attribute].CurrentValue.strip()
        # vectorIndex = setNullToEmptyString(cursors[Rule.VectorIndex].CurrentValue.strip())
        whereCondition = setNullToEmptyString(cursors[Rule.Where].CurrentValue.strip())
        wantedValue = cursors[Rule.Value].CurrentValue.strip().lower()
        ruleName = cursors[Rule.RuleName].CurrentValue.strip()

        moTuple = (moClass.lower(), attribute.lower())
        wantedValueType = attributeType[moTuple] if moTuple in attributeType else 'String'
        # print('wantedValueType: %s, wantedValue: "%s"' % (wantedValueType, wantedValue))
        invalidRuleFound = False
        cause = 'No cause'
        if moTuple not in attributeTable:
            invalidRuleFound = True
            cause = 'Invalid MO Class' if moClass not in moClasses else 'Invalid Attribute'
        elif wantedValueType == 'Boolean' and wantedValue != 'true' and wantedValue != 'false':
            invalidRuleFound = True
            cause = 'Boolean data type with invalid Value=%s' % wantedValue
        elif whereCondition and (whereCondition.isalpha()
                                 or not ('=' in whereCondition
                                         or '<>' in whereCondition
                                         or 'like' in whereCondition.lower()
                                         or ' in ' in whereCondition.lower())):
            invalidRuleFound = True
            cause = 'Missing conditional operator in Where clause'
        elif wantedValueType == 'Integer':
                # print('wantedValueType: ', wantedValueType)
                try:
                    value = int(wantedValue)  # try to cast to int to test data type
                    print(value, 'Integer data type with Value=%s' % value)
                except:
                    invalidRuleFound = True
                    cause = 'Integer data type with invalid Value=%s' % wantedValue
                    print(ruleName, 'Integer data type with invalid Value=%s' % wantedValue)
        if invalidRuleFound:
            invalidRulesWithCause.add(tuple([setNullToEmptyString(cursors[column].CurrentValue.strip()) for column in columns] + [cause]))

    print('invalidRulesWithCause: ', invalidRulesWithCause)
    return invalidRulesWithCause


# def addCauseInformation(validationStatusTable):
#     if not Document.Data.Tables.Contains(invalidRulesTableName):
#         print('Warning, missing invalid rules table')
#     else:
#         sourceTable = validationStatusTable
#         destinationTable = Document.Data.Tables[invalidRulesTableName]
#
#         # column matching between two tables
#         joinColumnName = Rule.RuleName
#         joinColumns = Dictionary[DataColumnSignature, DataColumnSignature]()
#         joinColumns.Add(DataColumnSignature(destinationTable.Columns[joinColumnName]), DataColumnSignature(sourceTable.Columns[joinColumnName]))
#
#         ignoredColumns = List[DataColumnSignature]()  # columns to ignore
#
#         settings = AddColumnsSettings(joinColumns, JoinType.LeftOuterJoin, ignoredColumns)
#         dataSource = DataTableDataSource(sourceTable)
#         destinationTable.AddColumns(dataSource, settings)


def fetchInvalidRules():
    invalidRules = set()
    if not Document.Data.Tables.Contains(invalidRulesTableName):
        print('Warning, missing invalid rules table')
    else:
        dataTable = Document.Data.Tables[invalidRulesTableName]
        rows = IndexSet(dataTable.RowCount, True)
        columns = [Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName, invalidCauseDescriptionColumnName]
        cursors = {column: DataValueCursor.CreateFormatted(dataTable.Columns[column]) for column in columns}
        for row in dataTable.GetRows(rows, Array[DataValueCursor](cursors.values())):
            invalidRules.add(tuple([cursors[column].CurrentValue if cursors[column].CurrentValue != '(Empty)' else '' for column in columns]))
        # print('Invalid Rules: ', invalidRules)
        # print('Invalid RuleNames: ', RuleNames)
    return invalidRules


def writeInvalidRules(invalidRules):
    print('invalidRules: ', invalidRules)
    dataTableName = invalidRulesTableName
    # Build a CSV table in memory and save to data table
    delimiter = '\t'
    stream = MemoryStream()
    csvWriter = StreamWriter(stream)
    # csvWriter.WriteLine('RuleName\tValidation Status')
    csvWriter.WriteLine(delimiter.join([Rule.MOClass, Rule.Attribute, Rule.ID, Rule.Value, Rule.VectorIndex, Rule.Where, Rule.RuleName, invalidCauseDescriptionColumnName]))
    for rule in sorted(invalidRules):
        # csvWriter.WriteLine('%s\t%s' % (rule, results[rule]))
        csvWriter.WriteLine(delimiter.join([str(x).replace('\n', ' ') for x in rule]))
    settings = TextDataReaderSettings()
    settings.Separator = delimiter
    settings.AddColumnNameRow(0)
    csvWriter.Flush()
    stream.Seek(0, SeekOrigin.Begin)
    textFileDataSource = TextFileDataSource(stream, settings)

    if Document.Data.Tables.Contains(dataTableName):  # If exists, replace it
        Document.Data.Tables[dataTableName].ReplaceData(textFileDataSource)
    else:  # If it does not exist, create new
        Document.Data.Tables.Add(dataTableName, textFileDataSource)

if applicationType == analysisWebClient:

    Rule = Rule()  # Rule column names
    Attribute = Attribute()  # Attribute column names
    Normal, Vector = range(2)  # Table types

    bulkCmTableForMO, attributeType, attributeTable, moClasses = fetchMOAttributeDetails()
    markedRulesBeforeCount = Document.Data.Markings[rulesMarkingName].GetSelection(rulesTable).AsIndexSet().Count  # get a count of the marked rules before any invalid rule cleaning happens


    existingInvalidRules = fetchInvalidRules()
    print('existingInvalidRules:', existingInvalidRules)
    # If there are any existing invalid rules that have been added back into the original table by mistake, then remove them
    cleanInvalidRules(existingInvalidRules)

    invalidRulesWithCause = validateRules(rulesTable, attributeType, attributeTable, moClasses)
    print("invalidRules after validation: ", invalidRulesWithCause)
    invalidRulesCount = len(invalidRulesWithCause)
    print("invalidRulesCount: ", invalidRulesCount)
    cleanInvalidRules(invalidRulesWithCause)
    markedRulesAfterCount = Document.Data.Markings[rulesMarkingName].GetSelection(rulesTable).AsIndexSet().Count  # get a count of the marked rules after any invalid rule cleaning happens

    # invalidRules = [rule for rule, _ in invalidRulesWithCause]
    # cleanInvalidRules(invalidRules)
    # addValidationStatus(invalidRulesWithCause)
    #
    if markedRulesBeforeCount and not markedRulesAfterCount:  # All marked rules were found to be invalid, so don't execute any rules
        print("Found %d invalid rules: " % invalidRulesCount)
    else:
        print('Find Discrepancies')
        blacklistNodes = getBlacklistedNodes(blacklistTableName)
        # print('bulkCmTableForMO: ', bulkCmTableForMO)
        # print('moClasses: ', moClasses)
        # print(blacklistNodes)

        # Fetch Discrepancies
        progressText = 'Fetching Discrepancies from %s ...' % dataSourceName
        replaceTableOnFirstQuery = [True]
        discrepancyQueries = getSQLForQuery(rulesTable, bulkCmTableForMO, selectedDate, blacklistNodes)
        fetchDataFromENIQ(discrepancyQueries, dataSourceName, discrepanciesTableName, progressText, replaceTableOnFirstQuery)
        switchToResultsPage()  # Show Statistics page

    print("invalidRules after execution: ", invalidRulesWithCause)
    if invalidRulesWithCause:
        writeInvalidRules(existingInvalidRules | invalidRulesWithCause)
        cleanInvalidRules(invalidRulesWithCause)

    print("Execution time --- %s seconds ---" % (time.time() - startTime))
