from Spotfire.Dxp.Data import *
myTable = Document.Data.Tables["Rules"]
rowSelection = Document.ActiveFilteringSelectionReference.GetSelection(myTable)

for marking in Document.Data.Markings:
	if marking == Document.Data.Markings["MarkingRules"]: 
		marking.SetSelection(rowSelection, myTable)