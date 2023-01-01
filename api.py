import json
import os,csv
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from flask import Flask, render_template, request, session , redirect, make_response, jsonify, send_file,send_from_directory,abort
from flask_sqlalchemy import SQLAlchemy
# from ssh_validate import validate_ssh_tunnel
from api_helper import validate_connection
from flask_cors import CORS
from Data_validation import do_compare
from config_1 import UPLOAD_FOLDER,Work_Folder,Reporting_folder,incl_cols_upload,excl_cols_upload,query_upload,incl_cols_report,BASE_DIR
from incl_columns_main_testing_only import wrappr_incl
from not_in_columns_main import wrapper_excl
from fact_query_testcase import wrapper_for_query
from BDV_main import do_compare1
from csv_cleanser_new import csv_cleanser


REQ_OBJECT = {}

staging_dir = UPLOAD_FOLDER
# working_dir = "C:\\Users\\muhfnu\\Desktop\\Desktop_muhfnu\\DFTE_Demo\\Table_Details\\output"
working_dir = Work_Folder

    
local_server = True
app = Flask(__name__)
CORS(app) 

@app.route('/upload_pkey', methods = ['POST'])
def upload_pkey():
    print(request.files)
    f = request.files['file']
    location = request.form['file_type']
    print(os.path.join(staging_dir,location,f.filename))
    ssh_key_path = os.path.join(staging_dir,location,f.filename)
    f.save(os.path.join(staging_dir,location,f.filename))
    return make_response(jsonify({"Success":"Private Key file uploaded successfully","ssh_key":ssh_key_path}))



# @app.route("/ssh_validate", methods = ["POST"])
# def val_ssh():
#     response = {}
#     response["message"] = "SSH tunnel Created Successfully"
#     data = request.get_json()
#     (result, exception) = validate_ssh_tunnel(data["ssh_details"])
#     print(result, exception)

#     return make_response(jsonify(response))



@app.route("/validate", methods = ['POST'])

def validate_conn():
    response = {}
    response["message"] = "Connection successful."
    data = request.get_json()
    print("data",data)
    # validate source connection
    
    (result, exception, conn_str) = validate_connection(data["source_connection"])
    print(result, exception)
    
    # validate target connection
    (result_1, exception_1, conn_str_1) = validate_connection(data["target_connection"])  
    print(result_1, exception_1)
    

    if result:
        if result_1:
            response["message"]= "Source and Target connection successful."


            REQ_OBJECT['conn_src']=conn_str
        
            REQ_OBJECT['conn_tar']=conn_str_1

            print("req object here >",REQ_OBJECT)
            
        else: 
            response["message"]= " Target connection Failed "
            response["error"] = str(exception_1)
    else:
        if result_1:
            
            response["message"]= " Source connection Failed"
            response["error"] = str(exception)
        else:
        
            response["message"]= "Connection failed for Target and Source || Access Denied "
            response["error"] = str(exception)
            
    return make_response(jsonify(response))

@app.route("/test_validation", methods = ['POST'])
def do_comparision():

    response1 = {}
    final_report_dict={}
    
    response1["message"] = "Validation Completed "
    request_obj = request.get_json()
    print("req object of test_validation function",request_obj)
    final_report_dict = do_compare(request_obj, staging_dir)
    print("FINAL Report dict",final_report_dict)
    
    # return make_response(jsonify(response1))
    return make_response(jsonify(final_report_dict))
    

@app.route('/uploader', methods = ['POST'])
def upload_file():
    print(request.files)
    f = request.files['file']
    location = request.form['file_type']
    print(os.path.join(staging_dir,location,f.filename))
    f.save(os.path.join(staging_dir,location,f.filename))
    return make_response(jsonify({"Success":"Source file uploaded successfully"}))

#  for target files..
@app.route('/uploaderT', methods = ['POST'])
def uploadT_file():
    # print(request.files)
    f1 = request.files['file']
    location = request.form['file_type']
    print(os.path.join(staging_dir,location,f1.filename))
    f1.save(os.path.join(staging_dir,location,f1.filename))
    return make_response(jsonify({"Success":"Target file uploaded successfully"}))

#  API for cleansing CSV files:

@app.route('/csv_cleanser', methods = ['POST'])
def clean_MDV_csv():
    response = {}
    a=csv_cleanser()
    print("str(a)",str(a))
    for k,v in a.items():
        print(k)
        print("v",str(v))
        if k =='error':
            response["message"] = str(v)
            response["error"] = "Error"
            return make_response(jsonify(response))
        else:
            response["Success"] = " No Tables Missing "
            return make_response(jsonify(response))
        
@app.route('/return-file', methods = ['GET'])
def return_file():
    print("PYTHON")
    return send_file(Reporting_folder+'\\FinalReport.xlsx',as_attachment=True,attachment_filename='FinalReport.xlsx') 	
    # return send_from_directory("C:\\Users\\muhfnu\\Desktop\\Table_Details\\TimeReport_Faisal.xlsx","TimeReport_Faisal.xlsx")

@app.route('/view-folder', methods = ['POST'])
def view_fol():
    
    data = request.get_json()
    print("data",format(data))
    d_folder_name=data['folder_name']
    print("folder name",d_folder_name)
    
    folder= BASE_DIR+'\\static\\MDV'
    file_path=os.path.join(folder,d_folder_name)
    print(file_path)
    os.startfile(file_path)
    return make_response(jsonify({"Success":"File opened"}))



# ////////////////////////////////// Including Columns .//////////////////////////////////////////////////////

@app.route('/uploader_incl', methods = ['POST'])
def upload_file_incl():
    print(request.files)
    f = request.files['file']
    location = request.form['file_type']
    print(os.path.join(incl_cols_upload,location,f.filename))
    f.save(os.path.join(incl_cols_upload,location,f.filename))
    return make_response(jsonify({"Success":"File uploaded successfully"}))


@app.route("/test_incl_cols", methods = ['POST'])
def val_incl_columns():
    # request_obj = request.get_json()
    print("reqobj",REQ_OBJECT)
    out_dict={}
    # out_dict = wrapper_incl(st,tt,sc,tc)
    out_dict=wrappr_incl(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    print("dict",out_dict)
    # return make_response(jsonify(response1))
    return make_response(jsonify(out_dict))


@app.route('/view-folder_incl', methods = ['POST'])
def view_fol_incl():
    
    data = request.get_json()
    print("data",format(data))
    d_folder_name=data['folder_name']
    print("folder name",d_folder_name)
    
    folder= BASE_DIR+'\\static\\ADV\\Including_Columns'
    file_path=os.path.join(folder,d_folder_name)
    print(file_path)
    os.startfile(file_path)
    return make_response(jsonify({"Success":"File opened"}))

# # ////////////////////////////////// Excluding Columns .//////////////////////////////////////////////////////


@app.route('/uploader_excl', methods = ['POST'])
def upload_file_excl():
    print(request.files)
    f = request.files['file']
    location = request.form['file_type']
    print(os.path.join(excl_cols_upload,location,f.filename))
    f.save(os.path.join(excl_cols_upload,location,f.filename))
    return make_response(jsonify({"Success":"File uploaded successfully"}))

@app.route("/test_excl_cols", methods = ['POST'])
def val_excl_columns():
    print("reqobj",REQ_OBJECT)
    out_dict={}
    # out_dict = wrapper_incl(st,tt,sc,tc)
    out_dict=wrapper_excl(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    print("dict",out_dict)
    # return make_response(jsonify(response1))
    return make_response(jsonify(out_dict))

@app.route('/view-folder_excl', methods = ['POST'])
def view_fol_excl():
    
    data = request.get_json()
    print("data",format(data))
    d_folder_name=data['folder_name']
    print("folder name",d_folder_name)
    
    folder= BASE_DIR+'\\static\\ADV\\Excluding_Columns'
    file_path=os.path.join(folder,d_folder_name)
    print(file_path)
    os.startfile(file_path)
    return make_response(jsonify({"Success":"File opened"}))

# # ////////////////////////////////// Using Query .//////////////////////////////////////////////////////

@app.route('/uploader_query', methods = ['POST'])
def upload_file_query():
    print(request.files)
    f = request.files['file']
    location = request.form['file_type']
    print(os.path.join(query_upload,location,f.filename))
    f.save(os.path.join(query_upload,location,f.filename))
    return make_response(jsonify({"Success":"File uploaded successfully"}))

@app.route("/test_query_inp", methods = ['POST'])
def val_query():
    print("reqobj",REQ_OBJECT)
    out_dict={}
    # out_dict = wrapper_incl(st,tt,sc,tc)
    out_dict=wrapper_for_query(src_engine=REQ_OBJECT['conn_src'],tar_engine=REQ_OBJECT['conn_tar'])
    print("dict",out_dict)
    # return make_response(jsonify(response1))
    return make_response(jsonify(out_dict))

@app.route('/view-folder_query', methods = ['POST'])
def view_fol_query():
    
    data = request.get_json()
    print("data",format(data))
    d_folder_name=data['folder_name']
    print("folder name",d_folder_name)
    
    folder= BASE_DIR+'\\static\\ADV\\Query_Method'
    file_path=os.path.join(folder,d_folder_name)
    print(file_path)
    os.startfile(file_path)
    return make_response(jsonify({"Success":"File opened"}))

# /////////////////////////////// BDV ///////////////////////////////////////////////

@app.route("/test_BDV", methods = ['POST'])
def val_BDV():
    print("BDV////////////////////////////////////////////")
    response1 = {}
    final_report_dict={}
    response1["message"] = "Validation Completed "
    request_obj = request.get_json()
    print("req object",request_obj)
    final_report_dict = do_compare1(request_obj, staging_dir)
    print("FINAL Report dict",final_report_dict)
    
    # return make_response(jsonify(response1))
    return make_response(jsonify(final_report_dict))


@app.route('/view-folder_bdv', methods = ['POST'])
def view_fol_bdv():
    
    data = request.get_json()
    print("data",format(data))
    d_folder_name=data['folder_name']
    print("folder name",d_folder_name)
    
    folder= BASE_DIR+'\\static\\BDV'
    file_path=os.path.join(folder,d_folder_name)
    print(file_path)
    os.startfile(file_path)
    return make_response(jsonify({"Success":"File opened"}))


if __name__ == '__main__':
    app.run(debug=True)
    