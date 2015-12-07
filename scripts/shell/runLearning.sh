#!/usr/bin/env bash
nodeNum=5
memorySize=12g
hdfsHost=hdfs://phazontan-nn1.tan.ygrid.yahoo.com:8020
hdfsRoot=/projects/dapper/prod//common/optimization/learning/trainingData/DONE
trainingData=sampledTrainingData1
productSubCatList=pSubcatList
h2oLauncherScript=h2oLauncher.sh
rScript=logisticRegression.R
ageCtrFile=ageCtr
genderCtrFile=genderCtr
ageGenCtrFile=ageGenCtr
geoCtrFile=geoCtr

python personalizedTargeting.py -n $nodeNum -m $memorySize --hdfshost $hdfsHost --hdfsroot $hdfsRoot --Trdata $trainingData --SubcatList $productSubCatList --H2OLauncherScript $h2oLauncherScript --rScript $rScript --ageCtrFile $ageCtrFile --genderCtrFile $genderCtrFile --ageGenCtrFile $ageGenCtrFile --geoCtrFile $geoCtrFile
