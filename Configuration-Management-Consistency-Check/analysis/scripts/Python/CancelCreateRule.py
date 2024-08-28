
def nav_to_cm_rules():
    for page in Document.Pages:
        if page.Title == 'CM Rule Manager':
            Document.ActivePageReference = page
            break


def cleanup():
    rule_inputs = ['RuleName', 'MOClassName', 'AttributeName', 'IDName', 'ValueName', 'VectorIndex', 'WhereConditionName', 'CommentName','TableName']
    for input in rule_inputs:
        Document.Properties[input] = ''

    Document.Properties['CreateRuleError'] = ''
    dataTable = Document.Data.Tables['CM Attributes']
    data_filtering_selection = Document.Data.Filterings["Filtering scheme (MOClass)"]
    filtering_scheme = Document.FilteringSchemes[data_filtering_selection]
    filtering_scheme[dataTable].ResetAllFilters()

cleanup()
nav_to_cm_rules()
