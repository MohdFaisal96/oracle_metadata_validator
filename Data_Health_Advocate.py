#  Test Connection
from api_helper import validate_connection
from Data_validation import do_compare
# from config_1 import UPLOAD_FOLDER,Work_Folder,Reporting_folder,excl_cols_report,incl_cols_report,query_report
from config_1 import *
from BDV_main import do_compare1
from not_in_columns_main import wrapper_excl
from incl_columns_main_testing_only import wrappr_incl
from fact_query_testcase import wrapper_for_query
from csv_cleanser_new import csv_cleanser
import sys,os
import pandas as pd

REQ_OBJECT = {}
staging_dir = UPLOAD_FOLDER
working_dir = Work_Folder

print("                                 Data Validation Accelerator \n")

print("Enter Source connections \n")

connectionType_s = input('Enter your Connection type (standard/SSH) : ')
# ssh_tunnel_host_s = input('Enter ssh tunnel host (enter None if unused) : ')
# ssh_port_number_s = input('enter ssh port number (enter None if unused) : ')
# ssh_username_s = input('enter ssh username (enter None if unused) : ')
# ssh_key_s = input('enter ssh key location (enter None if unused) : ')
database_type_s = input('enter Database type (oracle) : ')
hostname_s = input('enter hostname : ')
port_s = input('enter port number : ')
username_s = input('enter username : ')
password_s = input('enter password : ')
database_s = input('enter Database service name : ')

print("\n")

print("Enter Target connections \n")

connectionType_t = input('Enter your Connection type (standard/SSH) : ')
# ssh_tunnel_host_t = input('Enter ssh tunnel host (enter None if unused) : ')
# ssh_port_number_t = input('enter ssh port number (enter None if unused) : ')
# ssh_username_t = input('enter ssh username (enter None if unused) : ')
# ssh_key_t = input('enter ssh key location (enter None if unused) : ')
database_type_t = input('enter Database type (oracle) : ')
hostname_t = input('enter hostname : ')
port_t = input('enter port number : ')
username_t = input('enter username : ')
password_t = input('enter password : ')
database_t = input('enter Database service name : ')

# data = {'source_connection': {'connectionType': connectionType_s, 'ssh_tunnel_host': ssh_tunnel_host_s, 'ssh_port_number': ssh_port_number_s, 'ssh_username': ssh_username_s, 'ssh_key': ssh_key_s, 'database_type': database_type_s, 'hostname': hostname_s, 'port': port_s, 'username': username_s, 'password': password_s, 'database': database_s}, 'target_connection': {'connectionType': connectionType_t, 'ssh_tunnel_host': ssh_tunnel_host_t, 'ssh_port_number': ssh_port_number_t, 'ssh_username': ssh_username_t, 'ssh_key': ssh_key_t, 'database_type': database_type_t, 'hostname': hostname_t, 'port': port_t, 'username': username_t , 'password': password_t, 'database': database_t}} 

data = {'source_connection': {'connectionType': connectionType_s,'database_type': database_type_s, 'hostname': hostname_s, 'port': port_s, 'username': username_s, 'password': password_s, 'database': database_s}, 'target_connection': {'connectionType': connectionType_t,'database_type': database_type_t, 'hostname': hostname_t, 'port': port_t, 'username': username_t , 'password': password_t, 'database': database_t}} 

# print("data",data)

data_updated = {}

def validate_conn():
    response = {}
    (result, exception, conn_str) = validate_connection(data["source_connection"])
    # validate target connection
    (result_1, exception_1, conn_str_1) = validate_connection(data["target_connection"])  

    if result:
        if result_1:
            response["message"]= "Source and Target connection successful."

            REQ_OBJECT['conn_src']=conn_str
        
            REQ_OBJECT['conn_tar']=conn_str_1

            # print("req object here >",REQ_OBJECT)
            print(response["message"])
            
        else: 
            response["message"]= " Target connection Failed "
            response["error"] = str(exception_1)
            print(response["message"])
            print(response["error"])
            sys.exit()
    else:
        if result_1:
            
            response["message"]= " Source connection Failed"
            response["error"] = str(exception)
            print(response["message"])
            print(response["error"])
            sys.exit()
        else:        
            response["message"]= "Connection failed for Target and Source || Access Denied "
            response["error"] = str(exception)
            print(response["message"])
            print(response["error"])
            sys.exit()
            
    return response

x={}
print("\n")
x=validate_conn()
# print(x)
print("\n")


print("upload the input files in the static folder \n")

src_file = input('Enter the source input file name : '+'\n')
tar_file = input('Enter the target input file name : '+'\n')

# clen_file = input('Press Enter to Cleanse files')
def clean_MDV_csv():
    response = {}
    a=csv_cleanser()
    # print("str(a)",str(a))
    for k,v in a.items():
        # print(k)
        # print("v",str(v))
        if k =='error':
            response["message"] = str(v)
            response["error"] = "Error"
            return response
        else:
            response["Success"] = " No Tables Missing "
            return response

clen = clean_MDV_csv()

print("Please select the Validation to be performed: ")

selector = input('''

            Enter 1 for Metadata Validation 
            Enter 2 for Basic Data Validation 
            Enter 3 for Advanced Data Validation 
            Enter 4 to Exit
            ''')

def MDV():
    print("Meta Data Validation \n")
    data1 = data
    # print("data1 before update",data1)
    print("Select the Validations to be performed \n")
    print("""
            Table Structure Validation
            Constraints Validation
            Triggers Validation
            Sequences Validation
            Index Validation
            To Validate all press 1
            """)

    validation_strings = input('Enter the list of validations in a comma separated manner (*Please Select atleast 1) : ')
    if validation_strings == '1':
        val_list = ["Table Structure Validation","Constraints Validation","Triggers Validation","Sequences Validation","Index Validation"]
    else:
        val_list = validation_strings.split(",")

    # print("val_list",val_list)

    data1.update({"source_file_name":src_file})
    data1.update({"target_file_name":tar_file})
    data1.update({"Validations":val_list})

    MDV.dict = data1

    # print("new req object",data)

    final_report_dict={}
    final_report_dict = do_compare(data1, staging_dir)
    # print("FINAL Report dict",final_report_dict)
    return(final_report_dict)

def BDV():
    print("Basic Data Validation \n")
    data2 = data
    # print("Data2",data2)
    val_list = ["Table Structure Validation","Record Count validation","Records mismatch validation"]
    # print("val_list",val_list)

    data2.update({"source_file_name":src_file})
    data2.update({"target_file_name":tar_file})
    data2.update({"Validations":val_list})
    response1 = {}
    final_report_dict={}
    response1["message"] = "Validation Completed "
    final_report_dict = do_compare1(data2, staging_dir)
    # print("FINAL Report dict",final_report_dict)
    print(response1["message"])
    return(final_report_dict)


def attr_exlcusion():
    print("Attribute Exclusion \n")
    print("upload the input files in the static folder \n")
    attr_excl_file = input('Enter the input file name : '+'\n')
    # print("Req OBj",REQ_OBJECT)
    data3 = MDV.dict
    # print("data3",data3)
    data3.update({"file_name":attr_excl_file})
    # print("data3",data3)

    out_dict={}
    out_dict=wrapper_excl(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    # print("out_dict",out_dict['data'])
    status_data = out_dict['data']
    df = pd.DataFrame(status_data)
    df.to_csv(excl_cols_report+'\\Status_Report.csv', index = False, header=True)

    file_path = excl_cols_report+'\\'+'FinalReport.xlsx'
    if os.path.exists(file_path):
        os.remove(file_path)
    return out_dict

def attr_inclusion():
    print("Attribute Inclusion \n")
    print("upload the input files in the static folder \n")
    attr_incl_file = input('Enter the input file name : '+'\n')

    # print("Req OBj",REQ_OBJECT)
    data3 = MDV.dict
    # print("data3",data3)
    data3.update({"file_name":attr_incl_file})
    # print("data3",data3)
    out_dict={}
    # out_dict = wrapper_incl(st,tt,sc,tc)
    out_dict=wrappr_incl(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    status_data = out_dict['data']
    df = pd.DataFrame(status_data)
    df.to_csv(incl_cols_report+'\\Status_Report.csv', index = False, header=True)

    file_path = incl_cols_report+'\\'+'FinalReport.xlsx'
    if os.path.exists(file_path):
        os.remove(file_path)

    return out_dict

def test_query():
    print("Through Query \n")
    print("upload the input files in the static folder \n")
    query_file = input('Enter the input file name : '+'\n')

    # print("Req OBj",REQ_OBJECT)
    data3 = MDV.dict
    # print("data3",data3)
    data3.update({"file_name":query_file})
    # print("data3",data3)
    out_dict={}
    # out_dict = wrapper_incl(st,tt,sc,tc)
    out_dict=wrapper_for_query(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    status_data = out_dict['data']
    df = pd.DataFrame(status_data)
    df.to_csv(query_report+'\\Status_Report.csv', index = False, header=True)

    file_path = query_report+'\\'+'FinalReport.xlsx'
    if os.path.exists(file_path):
        os.remove(file_path)
    return out_dict


if selector == '1':
    x=MDV()
elif selector == '2':
    y=BDV()
elif selector == '3':
    ADV_selector = input('''
                            Enter 1 for Atrribute Exclusion
                            Enter 2 for Attribute Inclusion
                            Enter 3 for Query Method
    ''')
elif selector == '4':
    sys.exit()

selector_2 = input(''' 
                        To Proceed with Basic Data Validation enter 1
                        To Proceed with Advance Data Validation enter 2
                        To Exit enter 3
                        ''')

if selector_2 == '1':
    y=BDV()


elif selector_2 == '2':
    ADV_selector = input('''
                            Enter 1 for Atrribute Exclusion
                            Enter 2 for Attribute Inclusion
                            Enter 3 for Query Method
    ''')
    if ADV_selector == '1':
        exc = attr_exlcusion()

    selector_4 = input(''' 
                            To Proceed with Attribute Inclusion enter 1
                            To Proceed with Query Method enter 2
                            ''')

    if selector_4 == '1':
        inc = attr_inclusion()

    selector_5 = input (''' 
                            To Proceed with Query Method enter 1
                            To Exit enter 2
                            ''')

    if selector_5 == '1':
        que = test_query()

    elif selector_5 == '2':
        sys.exit()

elif selector_2 == '3':
    sys.exit()

selector_3 = input(''' 
                        To Proceed with Advance Data Validation enter 1
                        To Exit enter 2
                        ''')

    
if selector_3 == '1':
    ADV_selector = input('''
                            Enter 1 for Atrribute Exclusion
                            Enter 2 for Attribute Inclusion
                            Enter 3 for Query Method
    ''')
    
elif selector_3 == '2':
    sys.exit()

if ADV_selector == '1':
    exc = attr_exlcusion()



selector_4 = input(''' 
                        To Proceed with Attribute Inclusion enter 1
                        To Proceed with Query Method enter 2
                        ''')

if selector_4 == '1':
    inc = attr_inclusion()

selector_5 = input (''' 
                        To Proceed with Query Method enter 1
                        To Exit enter 2
                        ''')

if selector_5 == '1':
    que = test_query()

elif selector_5 == '2':
    sys.exit()


    





