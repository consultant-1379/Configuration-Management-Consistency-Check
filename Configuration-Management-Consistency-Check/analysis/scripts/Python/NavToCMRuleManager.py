from System import DateTime

def main():
    date = (Document.Properties["RefreshDate"]).split(' ') 
    currentDate = (str(DateTime.UtcNow)).split(' ')
    if date[0] != currentDate[0]:
          Document.Properties["RefreshDate"] = str(DateTime.UtcNow)
    for page in Document.Pages:
          if (page.Title == "CM Rule Manager"):
              Document.ActivePageReference=page
    cmrules = Document.Data.Tables['cmrules']
    cmrules.Refresh()

main()