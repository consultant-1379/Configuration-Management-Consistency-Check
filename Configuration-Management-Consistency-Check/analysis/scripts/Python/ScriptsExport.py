import time, os

basePath = 'C_COLON__SLASH_temp_SLASH_PMEx_scripts_SLASH_'.replace("_SLASH_", "/").replace('_COLON_', ":")

for sc in Document.ScriptManager.GetScripts():
    try:
        scriptType = sc.Language.Language
        if scriptType == "IronPython":
            scriptExt = ".py"
            folderName = "Python"
        elif scriptType == "JavaScript":
            scriptExt = ".js"
            folderName = "JavaScript"
        folderpath = "{basePath}{folderName}".format(basePath=basePath, folderName= folderName)
        if not os.path.exists(folderpath):
            os.makedirs(folderpath)
        filepath = "{folderpath}/{sc_Name}{scriptExt}".format(folderpath = folderpath, sc_Name = sc.Name, scriptExt= scriptExt)
        print(filepath)
        f = open(filepath, 'wb')
        f.write(sc.ScriptCode)
        time.sleep(0.1)
        f.close
        time.sleep(0.1)
    except Exception as e:
        print(sc.Name, e)