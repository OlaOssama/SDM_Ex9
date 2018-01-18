import argparse
import subprocess
import os
import zipfile
import json

def compressSubmission(files, filename):
    ZipFile = zipfile.ZipFile(filename, "w" )
    for f in files:
        ZipFile.write(f, os.path.basename(f), compress_type=zipfile.ZIP_DEFLATED)

def decompressSubmission(filename, outfolder):
    zf = zipfile.ZipFile(filename)
    zf.extractall(outfolder)
    zf.close()

def compileAndTest(input_folder, exercise_folder, params):
    classpathSeperator = ':'
    if os.name == 'nt':
        classpathSeperator = ';'

    java_files = ''
    for f in params['java_files']:
        java_file = os.path.join(input_folder,f)
        if os.path.exists(java_file):
            java_files += java_file + ' '

    # create compilation directory
    compileDir = os.path.join(input_folder, "bin/")
    if not os.path.exists(compileDir):
        os.makedirs(compileDir)

    # compile code
    testing_jar = os.path.join(exercise_folder, params['testing_jar'])
    cmd = params['javac_path'] + ' -d ' + compileDir + ' -cp ' + params['junit_jar_path'] + classpathSeperator + testing_jar + ' ' + java_files
    print("Running following command: ", cmd)
    proc = subprocess.Popen(cmd, shell=True).wait()

    # javac -d <compilation directory> -cp <junit-location>:<test-jar-location> <filenames>
    #e.g.: javac -d bin/ -cp ~/Downloads/junit-4.12.jar:SDM_Exercise_02_Solution.jar SQLInteger.java SQLVarchar.java RowPage.java HeapTable.java

    # run Junit test
    for junit_test in params['junit_test_fqns']:

        resultFile =  os.path.join(input_folder, 'result_' + junit_test + '.txt')
        open(resultFile, 'a').close()
        cmd = params['java_path'] + ' -cp ' + compileDir + classpathSeperator + params['hamcrest_jar_path'] + classpathSeperator +  params['junit_jar_path'] + classpathSeperator + testing_jar + ' org.junit.runner.JUnitCore '+ junit_test + ' > ' + resultFile + ' 2> ' + resultFile
        print("Running following command: ", cmd)
        proc = subprocess.Popen(cmd, shell=True).wait()
        # e.g. java -cp bin/:/Users/melhindi/Downloads/junit-4.12.jar:SDM_Exercise_02_Solution.jar:/Users/melhindi/Downloads/hamcrest-core-1.3.jar org.junit.runner.JUnitCore de.tuda.sdm.dmdb.test.TestSuiteDMDB
        # e.g. java -cp bin/:/Users/melhindi/Downloads/junit-4.12.jar:SDM_Exercise_02_Solution.jar:/Users/melhindi/Downloads/hamcrest-core-1.3.jar org.junit.runner.JUnitCore de.tuda.sdm.dmdb.test.storage.types.TestSQLInteger
        print("View result file "+ resultFile + " to see result of tests")

with open('./submission.json') as tasksFH:
    tasks = json.load(tasksFH)

# loop for future use, we expect only one element
for task in tasks:
    archive_name = task['name']+'_submission.zip'
    submission_folder = task['name']+'_submission'
    compressSubmission(task['params']['source_files'], archive_name)
    decompressSubmission(archive_name,submission_folder)
    compileAndTest(submission_folder, '.', task['params'])

print("THIS CODE IS PROVIDED AS-IS. PLEASE BE AWARE THAT YOU ARE RESPONSIBLE FOR CHECKING THE VALIDITY OF YOUR SUBMISSION!")
print("PLEASE DOUBLE CHECK THAT THE LATEST VERSION OF YOUR CODE IS INCLUDED IN THE GENERATED ZIP FILE")
print("PLEASE ADJUST ANY OF THE PATHS IN THE submission.json FILE IF REQUIRED")
print("ATTENTION CODE HAS BEEN TESTED ON MAC OS ONLY")
