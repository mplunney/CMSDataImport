# Script to ingest multi-tabbed Excel xlsx files into Exasol
# 2020-01-15 mpl PEV-1440

import os
import pandas as pd
from sqlalchemy import create_engine
import pyexasol
import time
from datetime import datetime

# start clock
start_time = time.perf_counter()

# set working directory, target file, staging csv file
os.chdir(r'\\filepath')
staging_csv = 'test.csv'
destinationSchema = 'DEV'
destinationTable = 'CMS_HRRP_ImportData'
# remove staging csv file if extant
if staging_csv in os.listdir():
    os.remove(staging_csv)

# dicts to map CMS HRRP cohorts to appropriate sheets in xlsx file and Exasol-friendly column names
cohort_sheets = {'AMI':'Table 3 Discharges AMI Readm','COPD':'Table 4 Discharges COPD Readm',
                 'HF':'Table 5 Discharges HF Readm','PN':'Table 6 Discharges PN Readm','HK':'Table 8 Discharges HK Readm'}

column_names = ['PatientMRNID','IndexAdmitDS','IndexDischargeDS','InclusionExclusionTXT','IndexStayFLG',
                        'IndexPrimaryDiagnosisCD','DischargeDispositionID','UnplannedReadmitFLG','PlannedReadmissionFLG',
                        'ReadmitAdmitDS','ReadmitDischargeDS','ReadmitPrimaryDiagnosisCD','IndexReadmitSameFacilityFLG',
                        'ReadmittingFacilityID']

# instantiate dict to be populated with one dataframe per sheet/cohort
stage_list = {}

# create function to iterate through tabs
def parse_df(cohort, sheet):
    with pd.ExcelFile(file) as xlsx:
        # remove rows not containing data, isolate columns for import, assign column names
        tab = pd.read_excel(xlsx,sheet_name=sheet,header=None,usecols=[3,5,6,7,8,9,10,11,12,13,14,15,16,17],
                            names=column_names,
                            # skip headers and footers
                            skiprows=7,skipfooter=10,
                            # date and flag fields have trailing whitespace
                            converters={'IndexAdmitDS': str.rstrip,'IndexDischargeDS': str.rstrip,
                                        'ReadmitAdmitDS': str.rstrip,'ReadmitDischargeDS': str.rstrip},
                            )
        # drop empty row, transform DS and FLGfields, and add cohort name to records
        tab = tab.drop(0)
        tab['IndexAdmitDS'] = pd.to_datetime(tab['IndexAdmitDS'],infer_datetime_format=True,errors='coerce')
        tab['IndexDischargeDS'] = pd.to_datetime(tab['IndexDischargeDS'],infer_datetime_format=True,errors='coerce')
        tab['ReadmitAdmitDS'] = pd.to_datetime(tab['ReadmitAdmitDS'],infer_datetime_format=True,errors='coerce')
        tab['ReadmitDischargeDS'] = pd.to_datetime(tab['ReadmitDischargeDS'],infer_datetime_format=True,errors='coerce')
        tab = tab.replace({'Yes ': True, 'No ': False})
        tab = tab.assign(CohortNM=f'{cohort}')
        stage_list[cohort] = tab

# parse sheets into data frames, one per sheet/cohort
for cohort, sheet in cohort_sheets.items():
    parse_df(cohort, sheet)

# combine list of dataframes and then write to csv
unionDF = pd.concat(stage_list,axis='index',join='outer',ignore_index=True)
unionDF.to_csv(staging_csv,index=False,encoding='utf-8',date_format='%Y-%m-%d')

# time check
process_time = time.perf_counter()
print('\nFile processing successfully completed in ' + str(round((process_time - start_time), 2))+' seconds or ' +
      str(round(((process_time - start_time)/60), 1))+' minutes')

# instantiate sqlalchemy db connection (32-bit ODBC EXASolution driver needs to be configured)
engine = create_engine('exa+pyodbc://exasol')
cx_ddl = engine.connect()

# create DDLs for destination tables, and change data types for Exasol compatibility
ImportDDL = pd.io.sql.get_schema(unionDF, destinationTable, con=cx_ddl)
ImportDDL = ImportDDL.replace('CREATE', 'CREATE OR REPLACE')
ImportDDL = ImportDDL.replace('"index" BIGINT,', '')
ImportDDL = ImportDDL.replace('BIGINT', 'DECIMAL(15,0)')
ImportDDL = ImportDDL.replace('TEXT', 'VARCHAR(254) UTF8')
ImportDDL = ImportDDL.replace('FLOAT', 'DECIMAL(15,3)')
cx_ddl.execute(f'''OPEN SCHEMA {destinationSchema};''')
cx_ddl.execute(ImportDDL)

# instantiate pyexasol connection
DSN = 'exasol'
# pwFile is a plain-text file containing AD password to keep it out of the code
pwFile = 'p.txt'
pwf = open(pwFile)
pw = pwf.read()
pwf.close()
cx_csv = pyexasol.connect(dsn=DSN, user='username', password=pw, schema=destinationSchema, compression=True, quote_ident=True)

# import csv to destination table and count records
cx_csv.import_from_file(open(staging_csv, 'rb'), destinationTable, import_params={'skip': 1})
count = cx_csv.execute(f'''SELECT COUNT(*) FROM {destinationSchema}."{destinationTable}";''').fetchone()
cx_csv.close()

# print stats
end_time = time.perf_counter()
print(str(count[0]) + ' records imported at ' + str(datetime.now())[:19] + ' in ' + str(round((end_time - start_time), 2)) +
      ' seconds or ' + str(round((end_time - start_time)/60, 1))+' minutes')
