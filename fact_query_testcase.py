import csv 
import os
import sqlalchemy
import cx_Oracle
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import openpyxl
from openpyxl import Workbook
from config_1 import query_report,query_upload,query_output

#  ======================================  Creating engines  ============================================

def fact_val(ft,cl,jc,wc,ft1,cl1,jc1,wc1,conn_src,conn_tar):
    
    bad_chars = ["[","]"]

    engine_src = create_engine(conn_src)
    
    conn_src  = engine_src.connect()

    engine_tar = create_engine(conn_tar)

    conn_tar = engine_tar.connect()

    
    #  Source table query:

    # fct_query="Select {1} from {0} {2} where {3}"
    fct_query="Select {1} from {0} {2} {3}"
    fct = fct_query.format(ft,cl,jc,wc)
    # print("source table is :",fct)
    fct1 = ''.join(i for i in fct if not i in bad_chars)
    # print("New format source, ", fct1)
    df_1 = pd.read_sql_query(fct1,conn_src)
    # print("src table  is here :",df_1)

    # target Table query:

    fct_query1="Select {1} from {0} {2} {3}"
    fct_t = fct_query1.format(ft1,cl1,jc1,wc1)
    # print("query2",fct_t)
    fct2 = ''.join(i for i in fct_t if not i in bad_chars)
    # print("New format target, ", fct2)
    df_2 = pd.read_sql_query(fct2,conn_tar)
    # print("target  is here:",df_2)

    listIdenticalCol = [col for col in list(df_1.columns) if col in list(df_2.columns)]
                    # print(listIdenticalCol)
    dfDifference = pd.concat([df_1,df_2],sort=True)
                    # print(dfDifference)
    dfDifference = dfDifference[listIdenticalCol]
                    # print(dfDifference)
    dfDifference = dfDifference.reset_index(drop=True)
                    # print(dfDifference)

    dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
    idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
    dfDifference=dfDifference.reindex(idx)

    # print("The Differnece in the tables is :",dfDifference)

    # if len(dfDifference.index) == 0:
    #     print("The Records are successfully Matching for tables:",ft+" and "+ft1)
    # else:
    #     print("Records are not matching",ft,ft1)
    
    return(dfDifference)

def wrapper_for_query(src_engine=None,tar_engine=None,testcase_status_list=None,testcase_status_report=None):

    path = query_upload + "\\input\\fact_input_using_query.csv"
    response_dict = {}
    response_dict["message"] = "status report"
    response_dict["status"]="success"
    
    testcase_status_list = []
    testcase_status_report = []

    k="Result"

    report_wb = Workbook()
    report_sheet = report_wb.active
    report_sheet.append(['Tables','Record Mismatch Validation'])
    report_wb.save(query_report+'\\'+'FinalReport.xlsx') 

    ft=[]
    cl=[]
    jc=[]
    wc=[]
    ft1=[]
    cl1=[]
    jc1=[]
    wc1=[]
    queries=[]

    

    with open(path) as csvfile:
        
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        for column in readCSV:
            query = {}
            query["S_Fact_Table"] = column[0]
            query["S_Columns"] = column[1]
            query["S_Join_condition"] = column[2]
            query["S_Where_Clause"] = column[3]
            query["T_Fact_Table"] = column[4]
            query["T_Columns"] = column[5]
            query["T_Join_condition"] = column[6]
            query["T_Where_Clause"] = column[7]
            queries.append(query)
            ft.append(column[0])
            cl.append(column[1])
            jc.append(column[2])
            wc.append(column[3])
            ft1.append(column[4])
            cl1.append(column[5])
            jc1.append(column[6])
            wc1.append(column[7])

    for index, Source_table in enumerate(ft):
        testcase_status = []
        # testcase_status.append(st)

        testcase_status_dict = {}
        testcase_status_dict["Table"]=Source_table
        # print("testcase_status_dict",testcase_status_dict)


        df1= fact_val(ft[index],cl[index],jc[index],wc[index],ft1[index],cl1[index],jc1[index],wc1[index],src_engine,tar_engine)
        if len(df1.index) == 0:
    
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

        create_folder(ft[index])
        write_data(ft[index],datetime.now().strftime(ft[index]+' %d-%m-%Y-%H-%M-%S.csv'),df1)

    try:
        response_dict = create_report(testcase_status_list,testcase_status_report)
        # print("RESPONSE IN DO COMPARE",response_dict)
        return response_dict
    except Exception as e:
        response_dict["error"] = str(e)
        # response_dict["message"] = str(e)
        return response_dict

def create_folder(table_name):
    if not(os.path.exists(os.path.join(query_output,table_name))):
        [os.makedirs(os.path.join(query_output,table_name))]

def write_data(table_name,file_name,df):
    count=0
    # print("in write data func",df[1])
    my_list = df.columns.values
    my_list = [str(my_list[x]) for x in range(len(my_list))]
    # print("my_list of column names",my_list)
    header = ",".join(my_list)
    # print("Header",header)

    np.savetxt(os.path.join(query_output,table_name,file_name),df, delimiter=',',header=header,fmt='%s') 
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
        rep_wb = openpyxl.load_workbook(query_report + '\\' + 'FinalReport.xlsx')
        rep_sheet = rep_wb.active
        for tables in testcase_status_list:
            rep_sheet.append(tables)
        rep_wb.save(query_report + '\\' + 'FinalReport.xlsx')
    except:
        response_dict["message"]="Exception occured, could not save the test report."
    return response_dict








#  uncomment the below if using SSH connections

# ft1=str(ft).split(',')
# print("TAble : ",ft)
# print("Columns :",cl)
# print("Join_condition :",jc)
# print("Where_Clause :",wc)

# //// uncoment the below if using SSH connections ///////////

# server = sshtunnel.SSHTunnelForwarder(
#     ('129.213.103.207',22),
#     ssh_username="opc",
#     ssh_pkey="C:/Users/muhfnu/Desktop/privateKey",
#     remote_bind_address=('10.60.120.18', 1521)
# )

# server.start()
# print("ssh tunneling port : ",server.local_bind_port)

# # DEV GEN2

# SQLALCHEMY_DATABASE_URI='oracle+cx_oracle://DEV_DW:Fedex123#@127.0.0.1:{0}/?service_name=pdb1.ociacoeexaclien.ociacoenpe.oraclevcn.com'.format(server.local_bind_port)
# engine_T = create_engine(SQLALCHEMY_DATABASE_URI)
# # print("Target_engine >>>>>",engine_T)
# conn=engine_T.connect()
# print("connection_string1 is Connected \n")

# #  DEV CLASSIC

# oracle_connection_string='oracle+cx_oracle://DEV_DW:Fedex123#@lpcldv0239m.us6.oraclecloud.com:1521/?service_name=PDBOBIADEV.us6.oraclecloud.com'
# engine_S = create_engine(oracle_connection_string)
# # print("Source_engine >>>>>>",engine_S)
# conn1  = engine_S.connect()
# print("connection_string2 is Connected \n")

# ///////////////////////////// uncomment above if using ssh connection ////////////////////

# conn.invalidate()
# conn1.invalidate()
# server.stop()










