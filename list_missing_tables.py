import logging
import csv
from config_1 import UPLOAD_FOLDER

src_path = UPLOAD_FOLDER + "\\source\\Tables_On_Premise_for_Metadata_Validation.csv"
tar_path = UPLOAD_FOLDER + "\\target\\Tables_On_Cloud_for_Metadata_Validation.csv"

def csv_show_missing_tables(source_missing_table_list=None,target_missing_table_list=None):
    response_dict = {}
    clean_data=[]
    source_table=[]
    target_table=[]
    temp_array=[]
    source_missing_table_list=[]
    target_missing_table_list=[]

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
        reader = csv.reader(fname,delimiter = ",")
        data = list(reader)
        row_count_src = len(data)
        print("row_count_src",row_count_src)

        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            source_table=list(filter(''.__ne__,clean_data))

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
        reader = csv.reader(fname,delimiter = ",")
        data = list(reader)
        row_count_tar = len(data)
        print("row_count_tar",row_count_tar)

        data=fname.readlines()
        for x in data:
            var=x.strip()
            target_table.append(var)
            temp_array=list(filter(''.__ne__,target_table))
            print('temp',temp_array)

        target_table = set(target_table)
        source_table = set(source_table)

    # /// IF number of rows are same:

    if row_count_src==row_count_tar:

        old_added = source_table - target_table
        old_removed = target_table - source_table

        try:
            for line in source_table :
                if line in old_added:
                    source_missing_table_list.append(line.strip())
                elif line in old_removed:
                    target_missing_table_list.append(line.strip())
        except Exception as e:
            response_dict["error"] = str(e)
            response_dict["message"] = source_missing_table_list
            return response_dict

        try:
            for line in target_table:
                if line in old_added:
                    source_missing_table_list.append(line.strip())
                elif line in old_removed:
                    target_missing_table_list.append(line.strip())
        
        except Exception as e:
            print("excetption",str(e))
            response_dict["error"] = str(e)
            response_dict["message"] = target_missing_table_list
            return response_dict

        print("Source : {}".format(source_missing_table_list))
        print("Target : {}".format(target_missing_table_list))
        
        error_dict = {"Source Files":source_missing_table_list,"Target files":target_missing_table_list}
        print("error dict",error_dict)
        
        res = not bool(error_dict) 

        print("dictionary empty ? :" + str(res)) 

        if res:
            return None
        else:
            return error_dict

    else:
        print("Number of Rows not matching")
        return {"Count in source file":row_count_src,"Count in Target file":row_count_tar,"List of tables in source":source_missing_table_list,"List of Tables in Target":target_missing_table_list}
    # return source_missing_table_list,target_missing_table_list


out = {}

out=csv_show_missing_tables()
print("out",out)