# ********************************************************************
# Ericsson Inc.                                                 SCRIPT
# ********************************************************************
#
#
# (c) Ericsson Inc. 2020 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Inc. The programs may be used and/or copied only with
# the written permission from Ericsson Inc. or in accordance with the
# terms and conditions stipulated in the agreement/contract under
# which the program(s) have been supplied.
#
# ********************************************************************
# Name    : RefreshHTML.py
# Date    : 26/04/2024
# Revision: 1.0
# Purpose : 
#
# Usage   : CMCC
#


from Spotfire.Dxp.Application.Visuals import VisualTypeIdentifiers, HtmlTextArea

for page in Application.Document.Pages:
    if page.Title == 'CM Rule Manager':
        for vis in page.Visuals:
            if vis and vis.TypeId == VisualTypeIdentifiers.HtmlTextArea and vis.Title in ['Rule Manager Actions']:
                vis.As[HtmlTextArea]().HtmlContent += " "
                break
        break

