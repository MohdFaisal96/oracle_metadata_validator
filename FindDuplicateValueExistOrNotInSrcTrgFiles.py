import logging
import csv


def dup_check(response):
    response_dict={}
    clean_data=[]
    clean_data_1=[]
    clean_data_2=[]
    clean_data_3=[]
    srcDup = 0
    trgDup = 0

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
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

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
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
                
    if srcDup > 0 and trgDup > 0:
        print("Source & Target Files are having Duplicates rows.")
        response_dict["message"] = {"Success":"Source & Target Files are having Duplicates rows."}
        return(response_dict)
    elif srcDup == 0 and trgDup == 0:
        print("Source & Target Files are not having duplicate rows.")
        response_dict["message"] = {"Failed":"Source & Target Files are not having duplicate rows."}
        return(response_dict)
    elif srcDup > 0:
        print("Source File is having duplicate rows.")
        response_dict["message"] = {"Failed":"Source File is having duplicate rows."}
        return(response_dict)
    elif trgDup > 0:
        print("Target File is having duplicate rows.")
        response_dict["message"] = {"Failed":"Target File is having duplicate rows."}
        return(response_dict)