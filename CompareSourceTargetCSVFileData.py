import logging
import csv


def csv_show_missing_tables(source_missing_table_list=None,target_missing_table_list=None):

    clean_data=[]
    source_table=[]
    target_table=[]
    temp_array=[]
    source_missing_table_list=[]
    target_missing_table_list=[]

    with open('Tables_On_Premise_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            clean_data.append(var)
            source_table=list(filter(''.__ne__,clean_data))
    print("source tables",source_table)

    with open('Tables_On_Cloud_for_Metadata_Validation.csv','r') as fname:
        data=fname.readlines()
        for x in data:
            var=x.strip()
            target_table.append(var)
            temp_array=list(filter(''.__ne__,target_table))
    print("target tables",target_table)
    
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

    print("Source : {}".format(source_missing_table_list))
    print("Target : {}".format(target_missing_table_list))

    error_dict = {"Source Files":source_table,"Target files":target_table,"Missing from Source":target_missing_table_list,"Missing from Target":source_missing_table_list}
    print("error dict",error_dict)

    return error_dict
    
dict1={}
dict1 = csv_show_missing_tables()
