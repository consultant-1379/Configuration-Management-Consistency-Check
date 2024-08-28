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
# Name    : RemoveNewRule.py
# Date    : 10/03/2023
# Revision: 1.0
# Purpose : 
#
# Usage   : CMCC Analysis
#

from Spotfire.Dxp.Data import *

import ast


# Add a reference to the data table in the script.
data_table = Document.Data.Tables["New Rules"]

# Retrieve the marking selection
markings = Document.Data.Markings["NewRuleMarking"].GetSelection(data_table)

# delete marked rows in the Table
data_table.RemoveRows(markings)