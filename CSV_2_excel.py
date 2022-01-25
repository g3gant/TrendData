"""
Program: Desigo CC 4.xx and 5.0 Trend Data convertor from CSV file into EXCEL files.
Author: Anton Gegeniger
e-Mail: anton.gegeniger@siemens.com
Package requirements:
    Python                  3.8 +
    pandas                  1.3.5
    numpy                   1.19.5
    xlwt                    1.3.0

"""
import numpy as np
import pandas
import glob
import time
import json
import os
import sys



#Get files in the directory
path = 'c:/trends'
processed = {}
settings = {'TrendPath':'c:/trends','OutputPath':'c:/trends/excel'}
settings_fn = './Settings.json'
processed_fn = './processed.json'


def ReadIntoTable(fileName):
    #read csv file into DataFrame
    trend_data = pandas.read_csv(fileName, sep = ';', header = 0)
    #convert Timestamp into DateTime format 
    trend_data['DateTime'] = pandas.to_datetime(trend_data['DateTime'])
    #drop duplicates in timestamp for the same points
    noDup = trend_data.drop_duplicates(subset=['DateTime','Data Source'], keep='first')
    #pivotig the table by TimeStamp and point names as rows
    pivot = pandas.pivot(noDup,index = 'DateTime',columns = 'Data Source', values = 'Value')
    
    colNames = pivot.columns
    #removing columns with NaN data if exist
    pivot = pivot.loc[:, pivot.columns.notnull()]
    #resampling table to 15 minutes period
    pivot = pivot.resample('15min').pad()
    #filling gaps with upstream data
    pivot.ffill(inplace = True)
    
    #converting numeric columns to Float64
    for col in pivot.columns:
        pivot[col] = pandas.to_numeric(pivot[col], errors = 'ignore')

    
    #cleaning columns names from 'System:ManagementView.....'
    oldNames = []
    for col in colNames:
        oldNames.append(col)

    newNames = CleanName(oldNames)
    pivot.columns = newNames
    return pivot

def CleanName(names):
    newNames = []
    for nam in names:
        if nam == None or len (str(nam)) < 5: continue
        pLeft = str(nam).rfind('.')
        sLeft = str(nam)[:pLeft]
        pRight = sLeft.rfind('.')+1
        sRight = sLeft[pRight:]
        newName = sRight
        newNames.append(newName)
    return newNames

def SaveProcessed():
    global processed
    global processed_fn
    json_process = json.dumps(processed, indent=2)
    with open(processed_fn,'w') as f:
        f.write(json_process)
        #settings = json_process

def ReadProcessed():
    json_pro ={}
    global processed
    global processed_fn
    try:
        f = open(processed_fn,'r')
        json_pro = json.load(f)
        processed = json_pro 
        print ('\033[94m' + 'Processed file has been found!'+'\033[0m')
        print ('\033[94m' + str(len(processed)) + ' files counted'+'\033[0m' )   
    except:
        print('\033[93m' + 'WARNING: Processed file is not found, new one will be generated' + '\033[0m')

def ReadSettings():
    json_str ={}
    global settings
    global settings_fn
    settings_path = settings_fn
    try:
        f = open(settings_path,'r')
        json_str = json.load(f)
        settings = json_str 
        print ('\033[92m' + '##################### SETINGS ####################'+'\033[0m')
        print ('\033[92m' + 'Settings file has been found!'+'\033[0m')
        print ('\033[92m' + 'Path to the Trends folder: ' + settings['TrendPath'] +'\033[0m' )   
        print ('\033[92m' + 'Path to the Excel Output folder: ' + settings['OutputPath'] +'\033[0m' )
        print ('\033[92m' + '################### END SETINGS ##################'+'\033[0m')
        print()
        print()
        time.sleep(3)
        return True
    except:
        print ('\033[93m' + '#################### SETINGS ####################'+'\033[0m')
        print('\033[93m' + 'WARNING: Settings file is not found, default settings will be used' + '\033[0m')
        print()
        print ('\033[93m' + 'Path to the Trends folder: ' + settings['TrendPath'] +'\033[0m' )   
        print ('\033[93m' + 'Path to the Excel Output folder: ' + settings['OutputPath'] +'\033[0m' )
        print ('\033[93m' + '################### END SETINGS #################'+'\033[0m')
        print()
        print()
        time.sleep(3)
        return False    

def SaveSettings():
    global settings_fn
    global settings
    json_process = json.dumps(settings, indent=2)
    with open(settings_fn,'w') as f:
        f.write(json_process)



# =====================  Main Program ========================

os.chdir(os.path.dirname(sys.argv[0]))
if not ReadSettings():
    SaveSettings()

ReadProcessed()

print ('\033[92m' + 'Waiting for new CSV files........'+'\033[0m')
while True:

    files = glob.glob(settings['TrendPath'] +'/*.csv')
    
    #Processed needs to be stored in the file, in case the program was restarted, it will not process all files over again
    pivot = pandas.DataFrame
    
    for file in files:
        if not processed.get(file):
            
            pivot = ReadIntoTable(file)
            tr_path = settings['TrendPath']
            out_path = settings['OutputPath']
            outFileName = file.replace(tr_path,out_path).lower().replace('.csv','.xlsx')
            
            try:
                pivot.to_excel(outFileName)
                processed[file] = True
                SaveProcessed()
                print ('\033[92m' + outFileName + "  Has been processed!" + '\033[0m')
                
            except:
                print ('\033[93m' + outFileName + "  Hasn't been saved! Make sure it is not open and folder is not setup as Read Only" + '\033[0m')
        

    time.sleep(10)







