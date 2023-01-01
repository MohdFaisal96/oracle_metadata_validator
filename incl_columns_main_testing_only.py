import csv 
import os
import cx_Oracle
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from config_1 import incl_cols_upload,incl_cols_report,incl_cols_output
import openpyxl
from openpyxl import Workbook

file_path = incl_cols_report+'\\'+'FinalReport.xlsx'

if os.path.exists(file_path):
    os.remove(file_path)



def incl_cols(st,tt,sc,tc,conn_src,conn_tar):
    
    # engine = create_engine('mysql+pymysql://root:Lamborgini1!@localhost/sales_dw')

    engine_src = create_engine(conn_src)
    
    conn  = engine_src.connect()

    engine_tar = create_engine(conn_tar)

    conn_tar = engine_tar.connect()

    
    src= "select {1} from {0}"
    tgt= "select {1} from {0}"

    # print("sourcetable>>>>",st)
    dfSourceTable=pd.read_sql(src.format(st,sc), conn)
    # print("source table>>",dfSourceTable)

    # print("targettable>>",tt)
    dfTargetTable=pd.read_sql(tgt.format(tt,tc), conn_tar)
    # print("target table>> ",dfTargetTable)
   
    #  processing the data

    listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]

    dfDifference = pd.concat([dfSourceTable,dfTargetTable],sort=True)

    dfDifference = dfDifference[listIdenticalCol]

    dfDifference = dfDifference.reset_index(drop=True)


    dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
    idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
    dfDifference=dfDifference.reindex(idx)

    # print("difference in the tables:",dfDifference)

    return dfDifference

    
    


def wrappr_incl(src_engine=None,tar_engine=None,testcase_status_list=None,testcase_status_report=None):

# path = "C:\\Users\\muhfnu\\Desktop\\Desktop_muhfnu\\DFTE_Demo\\Table_Details\\test_file.csv" 
    path = incl_cols_upload + "\\input\\test_file_for_attribute_inclusion.csv"
    response_dict = {}
    response_dict["message"] = "status report"
    response_dict["status"]="success"
    
    testcase_status_list = []
    testcase_status_report = []

    k="Result"

    report_wb = Workbook()
    report_sheet = report_wb.active
    report_sheet.append(['Table', 'Records mismatch validation'])
    report_wb.save(incl_cols_report+'\\'+'FinalReport.xlsx') 

    st=[]
    tt=[]
    sc=[]
    tc=[]


    # table names from csv

    with open(path) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        for column in readCSV:
            query = {}
            query["Source_tables"] = column[0]
            query["Source_Columns"] = column[1]
            query["Target_Tables"] = column[2]
            query["Target_columns"] = column[3]
            st.append(column[0])
            sc.append(column[1])
            tt.append(column[2])
            tc.append(column[3])

    
    # report_incl_list = []
    # report_incl_list.append(st)
    # print("report_incl_list",report_incl_list)

    
    # print("testcase_status_dict",testcase_status_dict)


    # report_chld = []
    for index, Source_tables in enumerate(st):
        testcase_status = []
        # testcase_status.append(st)

        testcase_status_dict = {}
        testcase_status_dict["Table"]=Source_tables
        # print("testcase_status_dict",testcase_status_dict)

        # report_incl = []

        df = incl_cols(st[index],tt[index],sc[index],tc[index],src_engine,tar_engine)
        if len(df.index) == 0:

            # report_incl.append(st[index])
            # report_incl.append('Pass')
            

            testcase_status.append("Pass")
            testcase_status_dict[str(k)]="Pass"

        else:
            # report_incl.append(st[index])
            # report_incl.append('Fail')

            testcase_status.append("Fail")
            testcase_status_dict[str(k)] = "Fail"

        testcase_status_list.append(testcase_status_dict)
        # testcase_status_report.append(Source_tables)
        testcase_status_report.append(testcase_status)
        create_folder(st[index])
        write_data(st[index],datetime.now().strftime(st[index]+' %d-%m-%Y-%H-%M-%S.csv'),df)

        # report_incl_list.append(report_incl)
    try:
        response_dict = create_report(testcase_status_list,testcase_status_report)
        # print("RESPONSE IN DO COMPARE",response_dict)
        return response_dict
    except Exception as e:
        response_dict["error"] = str(e)
        # response_dict["message"] = str(e)
        return response_dict

def create_folder(table_name):
    if not(os.path.exists(os.path.join(incl_cols_output,table_name))):
        [os.makedirs(os.path.join(incl_cols_output,table_name))]

def write_data(table_name,file_name,df):
    count=0
    # print("in write data func",df[1])
    my_list = df.columns.values
    my_list = [str(my_list[x]) for x in range(len(my_list))]
    # print("my_list of column names",my_list)
    header = ",".join(my_list)
    # print("Header",header)
    np.savetxt(os.path.join(incl_cols_output,table_name,file_name),df, delimiter=',',header=header,fmt='%s') 
    count=count+1

def create_report(testcase_status_list=None,testcase_status_report=None):
    response_dict = {}
    response_dict["message"] = "status report"
    response_dict["status"]="success"
    response_dict["data"]= testcase_status_list
    print("The status report is saved in the static Directory")
    # print("RESPONSE DICT FOR REPORT",response_dict)
    # print("TESTCASE_STATUS_REPORT",testcase_status_report)
    try:
        rep_wb = openpyxl.load_workbook(incl_cols_report + '\\' + 'FinalReport.xlsx')
        rep_sheet = rep_wb.active
        for tables in testcase_status_list:
            rep_sheet.append(tables)
        rep_wb.save(incl_cols_report + '\\' + 'FinalReport.xlsx')
    except:
        response_dict["message"]="Exception occured, could not save the test report."
    return response_dict






