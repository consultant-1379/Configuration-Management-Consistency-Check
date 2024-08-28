from Spotfire.Dxp.Framework.Library import LibraryManager, LibraryItemType, LibraryItem, LibraryItemRetrievalOption
import os

tableName = Document.Properties["TableNameRules"]
tableFilePath = Document.Properties["FilePathRules"]

tableToSave = Document.Data.Tables[tableName]
libraryFolder, fileName = os.path.split(tableFilePath)
print('Attempting to save %s to %s' % (tableName, libraryFolder))

(found, item) = Application.GetService(LibraryManager).TryGetItem(tableFilePath, LibraryItemType.SbdfDataFile, LibraryItemRetrievalOption.IncludeProperties)

if found:
    print('Saving %s to %s' % (tableName, libraryFolder))
    tableToSave.ExportDataToLibrary(item, fileName)



