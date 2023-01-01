import logging
import csv

def check_blanks(response=None):

    response_dict={}
    clean_data=[]
    clean_data_1=[]
    clean_data_2=[]
    clean_data_3=[]
    cntSrcNoBlank = 0
    cntTrgNoBlank = 0
    cntSrcWithBlank = 0
    cntTrgWithBlank = 0

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            clean_data_1=list(filter(''.__ne__,clean_data))
        for y in clean_data_1:
            cntSrcNoBlank = cntSrcNoBlank + 1
        

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data_2.append(var)
            clean_data_3=list(filter(''.__ne__,clean_data_2))
        for y in clean_data_3:
            cntTrgNoBlank = cntTrgNoBlank + 1
        

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            cntSrcWithBlank = cntSrcWithBlank + 1
        

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            cntTrgWithBlank = cntTrgWithBlank + 1
        

    #  & >> |
    if cntSrcWithBlank == cntSrcNoBlank | cntTrgWithBlank == cntTrgNoBlank:
        print("Source & Target File are not having any blank rows.")
        response_dict["message"] = {"Success":"Source & Target File are not having any blank rows."}
        return(response_dict)
    elif cntSrcWithBlank != cntSrcNoBlank & cntTrgNoBlank != cntTrgWithBlank:
        print("Source & Target File are having blank rows.")
        response_dict["message"] = "Source & Target File are having blank rows."
        return(response_dict)
    elif cntSrcWithBlank != cntSrcNoBlank:
        print("Source File is having blank row.")
        response_dict["message"] = "Source File is having blank row."
        return(response_dict)
    elif cntTrgWithBlank != cntTrgNoBlank:
        # print("cntTrgWithBlank",cntTrgWithBlank)
        # print("cntTrgNoBlank",cntTrgNoBlank)
        # print("Target File is having blank row.")
        response_dict["message"] = "Target File is having blank row."
        return(response_dict)
    elif cntSrcWithBlank == cntSrcNoBlank:
        print("Source File is not having blank row.")
    elif cntTrgWithBlank == cntTrgNoBlank:
        print("Target File is not having blank row.")

dict_1=check_blanks()
print(dict_1)