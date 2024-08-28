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
# Name    : NetAnDb_CreateTables.ps1
# Date    : 19/10/2023
# Revision: 1.0
# Purpose : Creates/Upgrades NetAn database tables required for CM Consistency Check
#
# Usage   : CMCC Analysis
#
$drive = (Get-ChildItem Env:SystemDrive).value

$logDir = $drive + "\Ericsson\NetAnServer\Logs"

$postgres_service = "postgresql-x64-" +(((Get-ItemProperty HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Postgres*).MajorVersion) | measure -Maximum).Maximum
$setLogName = 'CMConsistencyCheckInstallation.log'

$paramList = @{}
$paramList.Add('pgSqlService', $postgres_service)
$paramList.Add('database', 'netanserver_db')
$paramList.Add('serverInstance', 'localhost')
$paramList.Add('username', 'netanserver')
$envVariable = "NetAnVar"
$password = (New-Object System.Management.Automation.PSCredential 'N/A', $(Get-EnvVariable $envVariable)).GetNetworkCredential().Password

Function createTables() {
	param(
		[hashtable] $paramList
    )
    $logger_pre_install.logInfo("----------------Creating tables in the netanserver_pmdb database.-------------------")
	$logger_pre_install.logInfo("")
	$envVariable = "NetAnVar"
    $password = (New-Object System.Management.Automation.PSCredential 'N/A', $(Get-EnvVariable $envVariable)).GetNetworkCredential().Password
    $query = Get-Content "C:\Ericsson\tmp\cmconsistencycheck\resources\Create_netan_db_tables.sql" | Out-String

    $result = Invoke-UtilitiesSQL -Database "netAnServer_pmdb" -Username $paramList.username -Password $password -ServerInstance $paramList.serverInstance -Query $query -Action insert


    if ("False" -eq $result[0]){			
        $logger_pre_install.logError($MyInvocation, "Couldn't create netanserver_pmdb tables", $True)	
        exit
    }
    else {
        $logger_pre_install.logInfo("----------------Successfully created the netanserver_pmdb tables.-------------------")
    }
    
}


Function modifyTables() {
	param(
		[hashtable] $paramList
    )

		
	$logger_pre_install.logInfo("----------------Updating tables in the netanserver_pmdb database.-------------------")
	$logger_pre_install.logInfo("")
	$envVariable = "NetAnVar"
	$password = (New-Object System.Management.Automation.PSCredential 'N/A', $(Get-EnvVariable $envVariable)).GetNetworkCredential().Password
	$query = Get-Content "C:\Ericsson\tmp\cmconsistencycheck\resources\Upgrade_netan_db_tables.sql" | Out-String
	$result = Invoke-UtilitiesSQL -Database "netAnServer_pmdb" -Username $paramList.username -Password $password -ServerInstance $paramList.serverInstance -Query $query -Action insert
	
	if ("False" -eq $result[0]){			
		$logger_pre_install.logError($MyInvocation, "Couldn't update the tables in netanserver_pmdb database", $True)	
		$logger_pre_install.logError($MyInvocation, "$($result[1])")
		exit
	}
	else {
		$logger_pre_install.logInfo("----------------Successfully updated the tables in the netanserver_pmdb database.-------------------")
		$logger_pre_install.logInfo("")
	
	} 
	
	
}


Function main() {
	Param(
		[string] $argument
	)	
	if (($argument -ne "install") -and ($argument -ne "upgrade")) {
		
		$logger_pre_install.logError($MyInvocation, "Invalid argument", $True)
		return
	}

	if ($argument -eq "install") {
		createTables $paramList 
		
	}
	elseif ($argument -eq "upgrade") {
		modifyTables $paramList
	}
	
}

$global:logger_pre_install = Get-Logger("cmconsistencycheck-create-tables")
if ( -not (Test-FileExists($logDir))) {
	New-Item $logDir -ItemType directory | Out-Null
	$creationMessage = "Creating new log directory $($logDir)"
	$logger_pre_install.logInfo($creationMessage)
}

$logger_pre_install.setLogDirectory($logDir)
$logger_pre_install.setLogName($setLogName)



main $args[0]