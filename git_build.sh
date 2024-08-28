#!/bin/bash
 
if [ "$2" == "" ]; then
    	echo usage: "$0" \<Branch\> \<RState\>
    	exit 1
else
	export versionProperties=install/version.properties
	theDate=\#$(date +"%c")
	export theDate
	module=$1
	export branch=$2
	export workspace=$3
	
fi

#to  get product   number 
function getProductNumber {
		product=$(grep "$module" "$PWD"/build.cfg | awk -F " " '{print $3}')
}

function setRstate {
		revision=$(grep "$module" "$PWD"/build.cfg | awk -F " " '{print $4}')
	
	if git for-each-ref --sort=creatordate --format '%(refname)' refs/tags | grep "$product"-"$revision"; then
        	rstate=$(git for-each-ref --sort=creatordate --format '%(refname)' refs/tags | grep "${product}"-"${revision}" | tail -1 | sed s/.*-// | perl -nle 'sub nxt{$_=shift;$l=length$_;sprintf"%0${l}d",++$_}print $1.nxt($2) if/^(.*?)(\d+$)/';)
        else
		ammendment_level=12
	        rstate=$revision$ammendment_level
	fi
	mv Configuration-Management-Consistency-Check/build/feature-release-mssql.xml Configuration-Management-Consistency-Check/build/feature-release-mssql."${rstate}".xml
	mv Configuration-Management-Consistency-Check/build/feature-release-pgsql.xml Configuration-Management-Consistency-Check/build/feature-release-pgsql."${rstate}".xml
		echo "Building rstate:$rstate"
}


function Arm104nexusDeploy {
	RepoURL=https://arm1s11-eiffel013.eiffel.gic.ericsson.se:8443/nexus/content/repositories/assure-releases 

	GroupId=com.ericsson.eniq.netanserver.features
	ArtifactId=$module
	zipName=Configuration-Management-Consistency-Check
	
	echo "****"	
	echo "Deploying the zip /$zipName-19.4.zip as ${ArtifactId}${rstate}.zip to Nexus...."
        mv target/"$zipName"-24.1.zip target/"${ArtifactId}".zip
	echo "****"	

  	mvn -B deploy:deploy-file \
	        	-Durl=${RepoURL} \
		        -DrepositoryId=assure-releases \
		        -DgroupId=${GroupId} \
		        -Dversion="${rstate}" \
		        -DartifactId="${ArtifactId}" \
		        -Dfile=target/"${ArtifactId}".zip
		      
}

getProductNumber
setRstate

#add maven command here
mvn package

Arm104nexusDeploy

rsp=$?

if [ $rsp == 0 ]; then

  git tag "$product"-"$rstate"
  git pull
  git push origin "$product"-"$rstate" 

fi

exit $rsp


