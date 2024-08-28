from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Application.Filters import *

# Loop through all filtering schemes
for filteringScheme in Document.FilteringSchemes:
      # Loop through all data tables
      for dataTable in Document.Data.Tables:
         # Reset all filters
         filteringScheme[dataTable].ResetAllFilters()						
