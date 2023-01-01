import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
# print("directorty>",BASE_DIR)

# /// MDV ///

UPLOAD_FOLDER = BASE_DIR+'\\static\\MDV\\uploads' 
# print("upload folder",UPLOAD_FOLDER)
Work_Folder = BASE_DIR+'\\static\\MDV\\output'
Reporting_folder = BASE_DIR+'\\static\\MDV\\Report'

# /// BDV ///

UPLOAD_FOLDER = BASE_DIR+'\\static\\MDV\\uploads'
BDV_Output = BASE_DIR+'\\static\\BDV\\output'

# print("upload folder",UPLOAD_FOLDER)
Work_Folder_BDV = BASE_DIR+'\\static\\BDV\\output'
Reporting_folder_BDV = BASE_DIR+'\\static\\BDV\\Report'


# //// ADV ////

UPLOAD_FOLDER_ADV = BASE_DIR+'\\static\\ADV' 

# //// 1.INCLUDING COLUMNS //

incl_cols_upload = UPLOAD_FOLDER_ADV+'\\Including_Columns\\uploads'
incl_cols_report = UPLOAD_FOLDER_ADV+'\\Including_Columns\\Report'
incl_cols_output = UPLOAD_FOLDER_ADV+'\\Including_Columns\\output'
# //// 2.EXCL Columns ///

excl_cols_upload = UPLOAD_FOLDER_ADV+'\\Excluding_Columns\\uploads'
excl_cols_report = UPLOAD_FOLDER_ADV+'\\Excluding_Columns\\Report'
excl_cols_output = UPLOAD_FOLDER_ADV+'\\Excluding_Columns\\output'

#  /// 3.QUERY Method ///

# C:\Users\muhfnu\Desktop\PYTHON\ODI\copy_(FINAL)_workhere\static\ADV\Query_Method\output

query_upload = UPLOAD_FOLDER_ADV+'\\Query_Method\\uploads'
query_report = UPLOAD_FOLDER_ADV+'\\Query_Method\\Report'
query_output = UPLOAD_FOLDER_ADV+'\\Query_Method\\output'

REQ_OBJECT = 'abc'
