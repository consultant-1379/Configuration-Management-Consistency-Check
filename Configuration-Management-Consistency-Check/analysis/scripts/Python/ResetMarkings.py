from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import *

# Loop through each data table
for dataTable in Document.Data.Tables:
   # Navigate through each marking in a given data table
   for marking in Document.Data.Markings:
      # Unmark the selection
      rows = RowSelection(IndexSet(dataTable.RowCount, False))
      marking.SetSelection(rows, dataTable)
