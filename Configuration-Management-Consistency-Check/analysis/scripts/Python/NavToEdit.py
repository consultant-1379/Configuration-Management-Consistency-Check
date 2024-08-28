from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import *
from Spotfire.Dxp.Application import PanelTypeIdentifiers
from Spotfire.Dxp.Application.Filters import *
import Spotfire.Dxp.Application.Filters as filters

def nav_to_page():
    """
    navigates to the Edit rule page
    """
    for page in Document.Pages:
          if (page.Title == "Edit Rule"):
              Document.ActivePageReference=page


def main():
    """
    main flow
    """
    marking = Document.Properties["SelectedRule"]
    values = marking.split("&#")
    Document.Properties["IDName"] = values[0]
    Document.Properties["RuleName"] = values[1]
    Document.Properties["MOClassName"] = values[2]
    Document.Properties["AttributeName"] = values[3]
    Document.Properties["ValueName"] = values[4]
    Document.Properties["VectorIndex"] = values[5]
    Document.Properties["WhereConditionName"] = values[6]
    Document.Properties["CommentName"] = values[7]
    Document.Properties["RuleManagerPageName"] = values[8]
    Document.Properties["ruleid"] = values[9]
    Document.Properties["TableName"]=values[10]
    print "val 2", values[2]
    print "val 3", values[3]
    src_table = Document.Data.Tables['CM Attributes']
    node_filt=Document.FilteringSchemes[Document.Data.Filterings['Filtering scheme (MOClass)']][src_table][src_table.Columns["MOClass"]].As[ListBoxFilter]()
    node_filt.IncludeAllValues=False
    node_filt.SetSelection(Document.Properties["MOClassName"])


    src_table = Document.Data.Tables['CM Attributes']
    node_filt=Document.FilteringSchemes[Document.Data.Filterings['Filtering scheme (MOClass)']][src_table][src_table.Columns["Attribute"]].As[ListBoxFilter]()
    node_filt.IncludeAllValues=False
    node_filt.SetSelection(Document.Properties["AttributeName"])
    Document.Properties["moClassNameError"]=""
    Document.Properties["AttributeNameError"]=""
    
main()
nav_to_page()
