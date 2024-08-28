from Spotfire.Dxp.Data import *
from Spotfire.Dxp.Data.Import import SbdfLibraryDataSource
from Spotfire.Dxp.Framework.Library import LibraryManager, LibraryItemType, LibraryItem, LibraryItemRetrievalOption

tableFilePath = Document.Properties["FilePathRules"]
tableName = Document.Properties["TableNameRules"]

(found, item) = Application.GetService(LibraryManager).TryGetItem(tableFilePath,
                                                                  LibraryItemType.SbdfDataFile,
                                                                  LibraryItemRetrievalOption.IncludeProperties)
if found:
    ds = SbdfLibraryDataSource(item)
    Document.Data.Tables[tableName].ReplaceData(ds)

