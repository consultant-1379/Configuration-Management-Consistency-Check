#!/bin/bash
unset LS_COLORS
export PATH=/app/git/1.7.8/LMWP3/bin:$PATH
export M2_HOME=/proj/esjkadm100/tools/apache-maven-3.0.5
export PATH=$M2_HOME/bin:$PATH
module=$1
rstate=$2
product=$3
echo "${module}"
echo "${rstate}"
echo "${product}"
#  added a test  comment
function Arm104nexusDeploy {
        RepoURL=https://arm1s11-eiffel013.eiffel.gic.ericsson.se:8443/nexus/content/repositories/assure-releases
        GroupId=com.ericsson.eniq.netanserver.features
        ArtifactId=$module
        zipName=Configuration-Management-Consistency-Check

        echo "****"
        echo "Deploying the zip /$zipName-18A.zip as ${ArtifactId}${rstate}.zip to Nexus...."
        mv target/"$zipName"-24.1.zip target/"${ArtifactId}".zip
        echo "****"

        mvn deploy:deploy-file \
                        -Durl=${RepoURL} \
                        -DrepositoryId=assure-releases \
                        -DgroupId=${GroupId} \
                        -Dversion="${rstate}" \
                        -DartifactId="${ArtifactId}" \
                        -Dfile=target/"${ArtifactId}".zip

}
#add maven command here
mvn package


Arm104nexusDeploy

rsp=$?

if [ $rsp == 0 ]; then
  git remote set-url --push origin ssh://esjkadm100@gerrit.ericsson.se:29418/OSS/ENIQ-CR-Parent/OSS/com.ericsson.eniq/Configuration-Management-Consistency-Check
  git tag "$product"-"$rstate"
  git pull
  git push origin "$product"-"$rstate" 
fi

exit $rsp

#testingpipeline