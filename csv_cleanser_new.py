import logging
import csv
from config_1 import UPLOAD_FOLDER

#  Check 1

src_path = UPLOAD_FOLDER + "\\source\\Tables_On_Premise_for_Metadata_Validation.csv"
tar_path = UPLOAD_FOLDER + "\\target\\Tables_On_Cloud_for_Metadata_Validation.csv"

# src_path = 'Tables_On_Premise_for_Metadata_Validation.csv'
# tar_path = 'Tables_On_Cloud_for_Metadata_Validation.csv'

def count_check(src_count=0,tar_count=0):
    # response_dict={}
    cntvalSrc=0
    cntvalTrg=0

    with open(src_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            cntvalSrc = cntvalSrc + 1

    with open(tar_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            cntvalTrg = cntvalTrg + 1
    # print("cntvalSrc",cntvalSrc)
    # print("cntvalTrg",cntvalTrg)
    return cntvalSrc,cntvalTrg
# ////////////////////////////////////////////////////////////////////////////////
#  Check 2


def check_blanks(srcnoblank,srcblank,tarnoblank,tarblank):
    
    # response_dict={}
    clean_data=[]
    clean_data_1=[]
    clean_data_2=[]
    clean_data_3=[]
    cntSrcNoBlank = 0
    cntTrgNoBlank = 0
    cntSrcWithBlank = 0
    cntTrgWithBlank = 0

    with open(src_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            clean_data_1=list(filter(''.__ne__,clean_data))
        for y in clean_data_1:
            cntSrcNoBlank = cntSrcNoBlank + 1
        

    with open(tar_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data_2.append(var)
            clean_data_3=list(filter(''.__ne__,clean_data_2))
        for y in clean_data_3:
            cntTrgNoBlank = cntTrgNoBlank + 1
        

    with open(src_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            cntSrcWithBlank = cntSrcWithBlank + 1
        

    with open(tar_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            cntTrgWithBlank = cntTrgWithBlank + 1
    # print(cntSrcNoBlank)
    # print(cntSrcWithBlank)
    # print(cntTrgNoBlank)
    # print(cntTrgWithBlank)
    return cntSrcNoBlank,cntSrcWithBlank,cntTrgNoBlank,cntTrgWithBlank
# ///////////////////////////////////////////////////////////////////////////////////////////////
#  Check 3

def dup_check(src_dup,tar_dup):
    # response_dict={}
    clean_data=[]
    clean_data_1=[]
    clean_data_2=[]
    clean_data_3=[]
    srcDup = 0
    trgDup = 0

    with open(src_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            clean_data_1=list(filter(''.__ne__,clean_data))
    seen = set()
    dups = set()
    for word in clean_data_1:
            if word in seen:
                if word not in dups:
                    srcDup = 1
            else:
                seen.add(word)

    with open(tar_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data_2.append(var)
            clean_data_3=list(filter(''.__ne__,clean_data_2))
    seen = set()
    dups = set()
    for word in clean_data_3:
            if word in seen:
                if word not in dups:
                    trgDup = 1
            else:
                seen.add(word)
    
    return srcDup,trgDup
# //////////////////////////////////////////////////////////////////////////////////////////////

def csv_show_missing_tables(source_missing_table_list=None,target_missing_table_list=None):
    
    clean_data=[]
    source_table=[]
    target_table=[]
    temp_array=[]
    source_missing_table_list=[]
    target_missing_table_list=[]

    with open(src_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            source_table=list(filter(''.__ne__,clean_data))
    # print("source tables",source_table)

    with open(tar_path,'r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            target_table.append(var)
            temp_array=list(filter(''.__ne__,target_table))
    # print("target tables",target_table)
    
    target_table = set(target_table)
    source_table = set(source_table)

    old_added = source_table - target_table
    old_removed = target_table - source_table

    for line in source_table :
        if line in old_added:
            source_missing_table_list.append(line.strip())
        elif line in old_removed:
            target_missing_table_list.append(line.strip())

    for line in target_table:
        if line in old_added:
            source_missing_table_list.append(line.strip())
        elif line in old_removed:
            target_missing_table_list.append(line.strip())

    # print("Source : {}".format(source_missing_table_list))
    # print("Target : {}".format(target_missing_table_list))

    error_dict = {"The Source Files are ":list(source_table),"The Target files are ":list(target_table),"Missing from Source ":target_missing_table_list,"Missing from Target ":source_missing_table_list}
    # print("error dict",error_dict)

    return error_dict

# /////////////////////////////////////////////////////////////////////////////////

# MAIN method


def csv_cleanser ():

    response_dict={}
    out1 = 0
    out2 = 0
    out3 = 0
    out4 = 0
    dict_out = {}
    # Check 1

    (out1, out2)=count_check()
    if out1 == out2:

        # response_dict["message"] = {"Success":"Source & Target File row count is matching"}
        # return(response_dict)
        # check 2

        (out1,out2,out3,out4)=check_blanks(out1,out2,out3,out4)
        if out1 == out2 | out3 == out4:
            # check 3
        
            (out1, out2)=dup_check(out1,out2)
            if out1 > 0 and out2 > 0:
                # add next step here
                response_dict["error"] = "Source & Target Files are having Duplicate rows."
                return(response_dict)
            elif out1 == 0 and out2 == 0:
                dict_out = csv_show_missing_tables()
                # response_dict["message"] = {"Success":"Source & Target Files are not having duplicate rows."}
                
                return(dict_out)
            elif out1 > 0:
                response_dict["error"] = "Source File is having duplicate rows."
                return(response_dict)
            elif out2 > 0:
                response_dict["error"] = "Target File is having duplicate rows."
                return(response_dict)
            
            # response_dict["message"] = {"Success":"Source & Target File are not having any blank rows."}
            # return(response_dict)

        elif out1 != out2 & out3 != out4:
            response_dict["error"] = "Source & Target File are having blank rows."
            return(response_dict)
        elif out1 != out2:
            response_dict["error"] = "Source File is having blank row."
            return(response_dict)
        elif out3 != out4:
            response_dict["error"] = "Target File is having blank row."
            return(response_dict)

    else:
        # print("Not Matching")
        response_dict["error"] = "The count of Tables is not matching"
        return(response_dict)

# a=csv_cleanser_new()
# print(a)