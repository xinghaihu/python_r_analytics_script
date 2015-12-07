import subprocess
import sys
import os
import datetime
import commands
import time

'''
This script controls the flow for product ads recommendation.
The flow includes:
- R installation
- H2O installation
- H2O initialization
- Model training
- Upload result file to MOBSTOR

Usage example:
python personalizedTargeting.py -n 5 -m 12g --hdfshost hdfs://phazontan-nn1.tan.ygrid.yahoo.com:8020 --hdfsroot /user/xinghai --Trdata sampledTrainingData1 --SubcatList pSubcatList --H2OLauncherScript h2oLauncher.sh --rScript logisticRegression.R --ageCtrFile ageCtr --genderCtrFile genderCtr --ageGenCtrFile ageGenCtr --geoCtrFile geoCtr

author: @xinghai
'''

curDir = os.getcwd()

cmd_params = {"test":False, \
              "optimizationLog": "optimization.log", \
              "h2oInitLog": "h2oInit.log", \
              "nodeNum": "10", \
              "xmx": "12g", \
              "hdfsHost": "hdfs://phazontan-nn1.tan.ygrid.yahoo.com:8020", \
              "hdfsRoot": "/user/xinghai/", \
              "H2O_Launcher_Script": "h2oLauncher.sh", \
              "dataFile": "", \
              "subcatListFile": "", \
              "ageCtrFile": "", \
              "genderCtrFile": "", \
              "ageGenCtrFile": "", \
              "geoCtrFile": "", \
              "Rscript": "", \
              "mobstorURL": "", \
              "hdfsOutputDirName": "hdfsOutputDirName"}

def get_cmd_parames():
    idx = 1
    while idx < len(sys.argv):
        if sys.argv[idx].lower() == '-t' or sys.argv[idx].lower() == '--test':
            cmd_params["test"] = True
            idx += 1
        elif sys.argv[idx].lower() == '--log':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["optimizationLog"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--h2o_log':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["h2oInitLog"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '-n' or sys.argv[idx].lower() == '--node':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["nodeNum"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '-m' or sys.argv[idx].lower() == '--mxm':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["xmx"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--hdfshost':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["hdfsHost"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--hdfsroot':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["hdfsRoot"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--trdata':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["dataFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--subcatlist':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["subcatListFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--h2olauncherscript':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["H2O_Launcher_Script"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--rscript':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["Rscript"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--agectrfile':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["ageCtrFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--genderctrfile':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["genderCtrFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--agegenctrfile':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["ageGenCtrFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--geoctrfile':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["geoCtrFile"] = sys.argv[idx]
                idx += 1
        elif sys.argv[idx].lower() == '--mobstorurl':
            idx += 1
            if idx <= len(sys.argv):
                cmd_params["mobstorURL"] = sys.argv[idx]
                idx += 1
        else:
            idx += 1


def path_config():
    homeDir = curDir + "/.."
    srcDir = homeDir + "/src"
    libDir = homeDir + "/lib"
    outDir = homeDir + "/out"
    h2oLibDir = libDir + "/H2O"
    RLibDir = libDir + "/R"
    logDir = homeDir + "/log"
    tmpDir = homeDir + "/tmp"
    R_Install_Dir = RLibDir + '/install'

    if not os.path.exists(outDir):
        os.mkdir(outDir)

    if not os.path.exists(logDir):
        os.mkdir(logDir)

    if not os.path.exists(tmpDir):
        os.mkdir(tmpDir)

    if not os.path.exists(RLibDir):
        os.mkdir(RLibDir)

    if not os.path.exists(R_Install_Dir):
        os.mkdir(R_Install_Dir)

    R_source_tar = RLibDir + "/R-3.2.2.tar.gz"
    H2O_Zip_File = h2oLibDir + "/h2o-3.2.0.9-hdp2.2.zip"
    H2O_Install_Dir = h2oLibDir + "/h2o-3.2.0.9-hdp2.2"
    H2O_Hadoop_Driver_File = H2O_Install_Dir + "/h2odriver.jar"

    Model_Output_File = outDir + "/model.out"

    cmd_params["homeDir"] = homeDir
    cmd_params["srcDir"] = srcDir
    cmd_params["libDir"] = libDir
    cmd_params["h2oLibDir"] = h2oLibDir
    cmd_params["RLibDir"] = RLibDir
    cmd_params["logDir"] = logDir
    cmd_params["outDir"] = outDir
    cmd_params["tmpDir"] = tmpDir

    cmd_params["R_source_tar"] = R_source_tar
    cmd_params["R_Install_Dir"] = R_Install_Dir
    cmd_params["H2O_Zip_File"] = H2O_Zip_File
    cmd_params["H2O_Install_Dir"] = H2O_Install_Dir
    cmd_params["H2O_Hadoop_Driver_File"] = H2O_Hadoop_Driver_File
    cmd_params["Model_Output_File"] = Model_Output_File

    cmd_params["subcatListFile_local"] = cmd_params["tmpDir"] + "/" + cmd_params["subcatListFile"]
    cmd_params["subcatListFile_grid"] = cmd_params["hdfsHost"] + "/" + cmd_params["hdfsRoot"] + "/" + cmd_params["subcatListFile"]
    return

optLogFile = curDir + "/../log/optimization.log"
if os.path.exists(optLogFile):
    os.remove(optLogFile)
optF = file(optLogFile, 'a')

def check_r_installed(R_source_tar, r_Install_Success_Folder):
    if not os.path.exists(R_source_tar):
        err = 'R install file not exist on installation path. Exist.\n'
        optF.write("%s ERROR: %s" % ( str(datetime.datetime.now()), err) )
        sys.exit(err)
    return os.path.exists(r_Install_Success_Folder)

def check_r_decompressed(r_Install_Depressed_Folder):
    return os.path.exists(r_Install_Depressed_Folder)

def check_r_configed(r_Install_Configged_Folder):
    return os.path.exists(r_Install_Configged_Folder)

def install_r(R_Install_Dir, R_source_tar):
    r_Install_Success_Folder = R_Install_Dir + "/.success"
    if check_r_installed(R_source_tar, r_Install_Success_Folder):
        return
    r_Install_Depressed_Folder = R_Install_Dir + "/.decompress"

    if not check_r_decompressed(r_Install_Depressed_Folder):
        cmd_decompress_r = ['tar', '-xvf', R_source_tar, '--directory', R_Install_Dir, '--strip-components=1']
        subprocess.check_call( cmd_decompress_r, stdout = optF)
        optF.write("*"*100 + "\n")
        optF.write( str(datetime.datetime.now()) + " INFO: Decompress R with command: " + " ".join(cmd_decompress_r) + "\n")
        optF.write("*"*100 + "\n")
        os.system("mkdir " + r_Install_Depressed_Folder)

    os.chdir(R_Install_Dir)
    optF.write(str(datetime.datetime.now()) + " INFO: Enter R installation directory.\n")
    r_Install_Configged_Folder = R_Install_Dir + "/.configed"

    if not check_r_configed(r_Install_Configged_Folder):
        cmd_config_r = ['sh', './configure']
        subprocess.check_call(cmd_config_r, stdout = optF)
        optF.write(str(datetime.datetime.now()) + " INFO: Configure R.\n")
        os.system("mkdir " + r_Install_Configged_Folder)

    cmd_make_r = 'make'
    subprocess.check_call( cmd_make_r, stdout = optF )
    optF.write("*"*150 + "\n")
    optF.write(str(datetime.datetime.now()) + " INFO: Make R.\n")
    optF.write("*"*150 + "\n")
    os.system('mkdir ' + r_Install_Success_Folder)
    os.chdir(curDir)
    optF.write("*"*150 + "\n")
    optF.write(str(datetime.datetime.now()) + " INFO: Create .success folder and exit R installation directory. \n" )
    optF.write("*"*150 + "\n")
    optF.write("%s INFO: Installed R. \n" % str(datetime.datetime.now()) )
    return


def check_h2o_file_exist(H2O_Zip_File):
    return os.path.isfile(H2O_Zip_File)


def check_h2o_file_unzipped(H2O_Zip_Success_Folder):
    return os.path.exists(H2O_Zip_Success_Folder)


def unzip_h2o(H2O_Zip_Success_Folder, H2O_Zip_File):
    if check_h2o_file_unzipped(H2O_Zip_Success_Folder):
        return
    subprocess.check_call(['unzip', H2O_Zip_File], stdout=optF)
    os.system("mkdir " + H2O_Zip_Success_Folder)
    optF.write(str(datetime.datetime.now()) + ' INFO: Unzip ' + H2O_Zip_File + '\n')
    return


def initialize_h2o_from_hadoop(h2oLibDir, H2O_Zip_File, H2O_Install_Dir, H2O_Launcher_Script, HDFSRootDir, H2O_Init_Log_File, nodeNum, mapperXmx):
    if not check_h2o_file_exist(H2O_Zip_File):
        error = "Error: H2O installation file not exists."
        optF.write(str(datetime.datetime.now()) + " ERROR: " + error + "\n")
        sys.exit(error)
    H2O_Zip_Success_Folder = h2oLibDir + "/.zipped"
    os.chdir(h2oLibDir)
    unzip_h2o(H2O_Zip_Success_Folder, H2O_Zip_File)
    os.chdir(H2O_Install_Dir)
    optF.write(str(datetime.datetime.now()) + " INFO: Entering H2O installation directory. \n")
    hdfsOutputDirName = 'hdfsOutputDirName'
    HDFSOutputDir = HDFSRootDir + '/' + hdfsOutputDirName
    commands.getstatusoutput('hadoop fs -rmr ' + HDFSOutputDir)
    optF.write(str(datetime.datetime.now()) + " INFO: Remove hdfsOutputDirName. \n")
    optF.write(str(datetime.datetime.now()) + ' INFO: Enter H2O installation directory. \n' )
    H2O_Hadoop_Driver_File = H2O_Install_Dir + "/" + "h2odriver.jar"
    cmd_init_h2o = [ 'nohup', 'hadoop', 'jar', H2O_Hadoop_Driver_File,'-nodes', str(nodeNum), \
                     '-mapperXmx', mapperXmx, '-output', hdfsOutputDirName, \
                     '>', H2O_Init_Log_File, '2>&1 &']
    h2oLaucherF = open(H2O_Launcher_Script, 'w')
    h2oLaucherF.write(" ".join(cmd_init_h2o) + "\n")
    h2oLaucherF.write("exit")
    h2oLaucherF.close()
    optF.write(str(datetime.datetime.now()) + " INFO: Generate H2OLauncherFile.\n")
    optF.write(str(datetime.datetime.now()) + " INFO: Initialize H2O from Hadoop with command: " + " ".join(cmd_init_h2o) + "\n")
    os.system("sh " + H2O_Launcher_Script)
    i = 1
    while i <= 5 :
        time.sleep(60)
        h2oIp = get_h2o_host_info(H2O_Init_Log_File)
        if len(h2oIp) > 0:
            return h2oIp
        optF.write("wait for " + str(i*60) + " seconds...\n" )
        i += 1
    os.chdir(curDir)
    optF.write(str(datetime.datetime.now()) + " INFO: Successfully initialize H2O from Hadoop. \n" )
    e = "Fail to launch H2O. Exist.."
    optF.write(str(datetime.datetime.now()) + " ERROR: " + e + " \n" )
    sys.exit(e)
    return "-1"


def get_h2o_host_info(H2O_Init_Log_File):
    if not os.path.isfile(H2O_Init_Log_File):
        return ''
    f = open(H2O_Init_Log_File, 'r+')
    ip = ''
    while 1:
        line = f.readline()
        if not line:
            break
        else:
            if line.find('Open H2O Flow in your web browser') >= 0 :
                parts = line.split(':')
                ip = parts[2].strip('/ \n')
                break
    f.close()
    return ip


def get_training_data_path(hadoopDir, hadoopTrDataPath):
    return hadoopDir + hadoopTrDataPath


def get_testing_data_path(hadoopDir, hadoopTeData):
    return hadoopDir + hadoopTeData

def downloadSubcatListFile(subcatListFile_local, subcatListFile_grid):
    if os.path.exists(cmd_params["subcatListFile_local"]):
        os.remove(cmd_params["subcatListFile_local"])
    os.system("hadoop fs -copyToLocal %s %s" %(cmd_params["subcatListFile_grid"], cmd_params["subcatListFile_local"] ))
    optF.write(str(datetime.datetime.now()) + " INFO: Download subcategory list file to local.\n"  )
    if os.path.exists(cmd_params["subcatListFile_local"]):
        err = "local subcat list file missing."
        optF.write("%s ERROR: " %(str(datetime.datetime.now())) + err + "\n")
    return


def train_model(RinstallRepo, Rscript, h2oRSourceFilePath, h2oIP, h2oPort,  trainingDataPath, outputFile, productSubcatListFile):
    cmd = ""
    cmd += RinstallRepo
    cmd += '/bin/Rscript '
    cmd += Rscript
    cmd += ' '

    cmd += '--h2oRFilePath='
    cmd += h2oRSourceFilePath
    cmd += ' '

    cmd += '--h2oIP='
    cmd += h2oIP
    cmd += ' '

    cmd += '--h2oPort='
    cmd += h2oPort
    cmd += ' '

    cmd += '--datapath='
    cmd += trainingDataPath
    cmd += ' '

    cmd += '--out='
    cmd += outputFile
    cmd += ' '

    cmd += '--log='
    # cmd += cmd_params["logDir"] + "/" + time.strftime("%Y%m%d_%H%M%S") + ".R.log"
    cmd += cmd_params["logDir"] + "/" + "R.out.log"
    cmd += ' '

    cmd += '--productSubcatListFile='
    cmd += productSubcatListFile
    cmd += ' '

    commands.getstatusoutput( cmd )
    optF.write(str(datetime.datetime.now()) + " INFO: Train model with command: " + cmd + "\n"  )
    return

def getYcaCert():
    cmd = ["yca-cert-util", "--show", "yahoo.mobstor.client.ycreativeprod.prod"]
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    out, err = p.communicate()
    yca_cert = out.strip('\n ')
    return yca_cert

def uploadFileToMobstor(fileName, yca_cert):
    if not os.path.exists(fileName):
        optF.write(str(datetime.datetime.now()) + " ERROR: " + file + " is missing.\n"  )
    else:
        Yahoo_App_Auth = "Yahoo-App-Auth:" + yca_cert
        x_ysws_version = "x-ysws-version:1.0"
        Content_Type = "Content-Type:application/json"
        x_ysws_access = "x-ysws-access:public"
        Cache_Control = "Cache-Control:no-store"
        uploadToMobstorCommandList_DELETE = ["curl", cmd_params["mobstorURL"], "-X", "DELETE", "-T", fileName, "-H", Yahoo_App_Auth, "-H", x_ysws_version, "-H", Content_Type, "-H", x_ysws_access, "-H", Cache_Control]
        uploadToMobstorCommand_DELETE = " ".join(uploadToMobstorCommandList)
        uploadToMobstorCommandList_PUT = ["curl", cmd_params["mobstorURL"], "-X", "PUT", "-T", fileName, "-H", Yahoo_App_Auth, "-H", x_ysws_version, "-H", Content_Type, "-H", x_ysws_access, "-H", Cache_Control]
        uploadToMobstorCommand_PUT = " ".join(uploadToMobstorCommandList)
        os.system(uploadToMobstorCommand_DELETE)
        os.system(uploadToMobstorCommand_PUT)
        optF.write(str(datetime.datetime.now()) + " INFO: Upload File " + file + " to Mobstor with command: " + uploadToMobstorCommand + ".\n"  )
    return

def uploadAllOutputFileToMobstor(fileList):
    yca_cert = getYcaCert()
    for fileName in fileList:
        uploadFileToMobstor(fileName, yca_cert)
    return

def downloadCtrFiles():
    ageCtrFile = cmd_params["hdfsHost"] + cmd_params["hdfsRoot"] + "/" + cmd_params["ageCtrFile"] + "/part-r-00000"
    genderCtrFile = cmd_params["hdfsHost"] + cmd_params["hdfsRoot"] + "/" + cmd_params["genderCtrFile"] + "/part-r-00000"
    ageGenCtrFile = cmd_params["hdfsHost"] + cmd_params["hdfsRoot"] + "/" + cmd_params["ageGenCtrFile"] + "/part-r-00000"
    geoCtrFile = cmd_params["hdfsHost"] + cmd_params["hdfsRoot"] + "/" + cmd_params["geoCtrFile"] + "/part-r-00000"

    ageCtrFileLocal = cmd_params["outDir"] + "/" + cmd_params["ageCtrFile"]
    if os.path.exists(ageCtrFileLocal) :
        os.remove(ageCtrFileLocal)
    os.system("hadoop fs -copyToLocal %s %s" %(ageCtrFile, ageCtrFileLocal ))

    genderCtrFileLocal = cmd_params["outDir"] + "/" + cmd_params["genderCtrFile"]
    if os.path.exists(genderCtrFileLocal) :
        os.remove(genderCtrFileLocal)
    os.system("hadoop fs -copyToLocal %s %s" %(genderCtrFile, genderCtrFileLocal ))

    ageGenCtrFileLocal = cmd_params["outDir"] + "/" + cmd_params["ageGenCtrFile"]
    if os.path.exists(ageGenCtrFileLocal) :
        os.remove(ageGenCtrFileLocal)
    os.system("hadoop fs -copyToLocal %s %s" %(ageGenCtrFile, ageGenCtrFileLocal ))

    geoCtrFileLocal = cmd_params["outDir"] + "/" + cmd_params["geoCtrFile"]
    if os.path.exists(geoCtrFileLocal) :
        os.remove(geoCtrFileLocal)
    os.system("hadoop fs -copyToLocal %s %s" %(geoCtrFile, geoCtrFileLocal ))
    optF.write(str(datetime.datetime.now()) + " INFO: Downloaded CTR files to local.\n"  )
    return

def main():
    get_cmd_parames()
    path_config()
    optF.write("*" * 150 + "\n")
    optF.write("%s INFO: parameters are: \n %s . \n" %(str(datetime.datetime.now()), "\n".join([ '\t %s=%s' %x for x in cmd_params.items() ] ) ))
    optF.write("*" * 150 + "\n")

    install_r( cmd_params["R_Install_Dir"], cmd_params["R_source_tar"] )
    H2O_IP = initialize_h2o_from_hadoop( cmd_params["h2oLibDir"], cmd_params["H2O_Zip_File"], cmd_params["H2O_Install_Dir"], cmd_params["srcDir"]+"/"+cmd_params["H2O_Launcher_Script"], cmd_params["hdfsHost"]+cmd_params["hdfsRoot"], cmd_params["logDir"]+"/"+cmd_params["h2oInitLog"], cmd_params["nodeNum"], cmd_params["xmx"] )

    downloadSubcatListFile(cmd_params["subcatListFile_local"], cmd_params["subcatListFile_grid"])

    train_model(cmd_params["R_Install_Dir"], cmd_params["srcDir"]+"/"+cmd_params["Rscript"], cmd_params["h2oLibDir"]+"/h2o-3.2.0.9-hdp2.2/R/h2o_3.2.0.9.tar.gz", H2O_IP, "54321", cmd_params["hdfsHost"]+cmd_params["hdfsRoot"]+"/"+cmd_params["dataFile"], cmd_params["Model_Output_File"], cmd_params["subcatListFile_local"] )

    downloadCtrFiles()
    os.chdir(cmd_params["outDir"])
    outputFileList = [ cmd_params["ageCtrFile"], cmd_params["genderCtrFile"], cmd_params["ageGenCtrFile"], cmd_params["geoCtrFile"], cmd_params["Model_Output_File"]  ]
    uploadAllOutputFileToMobstor(outputFileList)
    optF.write(str(datetime.datetime.now()) + " INFO: Exit.\n"  )
    optF.close()
    return

if __name__ == '__main__':
    main()
