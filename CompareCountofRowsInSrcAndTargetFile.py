import logging
import csv

def count_check(response=None):
    response_dict={}
    cntvalSrc=0
    cntvalTrg=0

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
    for x in data:
        cntvalSrc = cntvalSrc + 1

    # print(cntvalSrc)

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            cntvalTrg = cntvalTrg + 1
    # print(cntvalTrg)

    if cntvalSrc == cntvalTrg:
        response_dict["message"] = {"Success":"Source & Target File row count is matching"}
        return(response_dict)
        # print("Matching")
    else:
        # print("Not Matching")
        response_dict["message"] = {"Failed":"The count is not matching"}
        return(response_dict)

dict2=count_check()
print(dict2)