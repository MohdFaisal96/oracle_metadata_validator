import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
# import cx_Oracle
import csv
import sys
import queries as qt
import numpy as np


# engine = create_engine('postgresql+pg8000://scott:tiger@localhost/mydatabase')         // for POSTGRE Database 
# engine = create_engine('mysql+pymysql://scott:tiger@localhost/foo')                   // for MySQl Database
# engine = create_engine('mssql+pymssql://scott:tiger@hostname:port/dbname')            // for MS-sql database
# engine = create_engine('sqlite:///C:\\path\\to\\foo.db')                              // for Sql-lite database
# engine = create_engine('oracle://faizal:abc@localhost:1521/orcl')
# engine = create_engine('mysql+pymysql://root:root@123@localhost/datavalidation')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('test.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
# file_handler = logging.FileHandler('error.log')
# file_handler.setLevel(logging.ERROR)
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)



class QualityChecker():
    def __init__(self, data):
        self.source_engine = self.get_engine(data["source_connection"])
        self.target_engine = self.get_engine(data["target_connection"])
    
    
    def get_engine(self, connection_details):
        if connection_details:
            if connection_details["database_type"] == "oracle":
                conn_str = "oracle://{0}:{1}@{2}:{3}/{4}".format(connection_details["username"],
                                                                    connection_details["password"],
                                                                    connection_details["hostname"],
                                                                    connection_details["port"],
                                                                    connection_details["database"])
                logger.info("Debug message here")
                logger.debug(conn_str)
                engine = create_engine(conn_str)
                return engine
                # connection  = engine.connect()
                # print(engine)
                # if connection:
                #     return (True,None)
                # else:
                #     print("Source Connection Failed")
        

   
    # case 1:  UNMATCHED COLUMNS

    def unmatch_check(self, st,tt):
        error_dict={}
        try:
            dfSourceTable = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            dfTargetTable = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)
            listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]
            # print("listIdenticalCol",listIdenticalCol)
            dfSourceCopy = dfSourceTable[listIdenticalCol].copy()
            dfTargetCopy = dfTargetTable[listIdenticalCol].copy()
            dfDifference = pd.concat([dfSourceCopy,dfTargetCopy],sort=True)
            # print("differnece dataframe column", list(dfDifference.columns))
            dfDifference = dfDifference[listIdenticalCol]
            dfDifference = dfDifference.reset_index(drop=True)
            dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
            idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
            dfDifference=dfDifference.reindex(idx)
            # print("DIFFERENCE",dfDifference)
            
        except Exception as e:
            dfDifference=pd.DataFrame()
            logger.exception(e)
            error_dict['error'] = "exception occured in unmatch_check"
        
        return st,dfDifference,error_dict
            
        # pass

    ## CASE 2: To check the count of Rows in target and source table

    def row_count_check(self, st,tt):
        dfobj1 = pd.DataFrame()
        error_dict={}
        try:
            df2 = pd.read_sql_query(qt.ROW_COUNT_CHECK.format(st),self.source_engine)
            # logger.info (df2)
            df2t = pd.read_sql_query(qt.ROW_COUNT_CHECK1.format(tt),self.target_engine)
            # logger.info (df2t)
            # logger.info(df2.iloc[0,0])
            # logger.info(df2t.iloc[0,0])
            df2_size = df2.iloc[0,0]
            df2t_size = df2t.iloc[0,0]
            if (df2_size == df2t_size).all().all():
                # print("hey")
                return (st,dfobj1,error_dict)
            else:
                # print("hey wow")
                return (st,df2,error_dict)
        except Exception as e:
            logger.exception(e)
            error_dict['error'] = "exception occured in rowcount_check"
            return st,dfobj1,error_dict
        
        # return (st,res)
        # pass

    ## CASE 3: to check all constraints in the two tables

    def constraint_check(self, st,tt):
        error_dict={}
        # error_dict1={}
        df_final_output = pd.DataFrame()
        try:
            #  Check if Table exist
            # print("query",qt.CON1_CHECK.format(st))
            # print("query",qt.CON1_CHECK.format(tt))

            dfSourceTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            dfTargetTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)

            # print("hi")
            df3 = pd.read_sql_query(qt.CON1_CHECK.format(st),self.source_engine)
            # print("df3",df3)
            df4 = pd.read_sql_query(qt.CON1_CHECK.format(tt),self.target_engine)
            # print("df4",df4)

            logger.info(df3)
            logger.info(df4)
            df3 = df3[['table_name','column_name','key_type','fk_to_table','constraint_name']]
            # print("DATAFRAME FROM DICT TARGET", df3)
            df4 = df4[['table_name','column_name','key_type','fk_to_table','constraint_name']]
            # print("DATAFRAME FROM DICT TARGET", df4)
            df3_copy = df3[df3.columns[1:4]]
            # print("DATFRAME SOURCE WITHOUT TABLE NAME",df3_copy)
            df4_copy = df4[df4.columns[1:4]]
            # print("DATFRAME TARGET WITHOUT TABLE NAME",df4_copy)
            dfDifference1 = pd.concat([df3_copy, df4_copy], ignore_index=True)
            dfDifferencegrpby1 = dfDifference1.groupby(list(dfDifference1.columns))
            idx1 = [x[0] for x in dfDifferencegrpby1.groups.values() if len(x) == 1]
            dfDifference1=dfDifference1.reindex(idx1)
            logger.info(dfDifference1)
            if dfDifference1.empty != True:
                df_check_table_source = pd.concat([df3_copy, dfDifference1], ignore_index=True)
                dfDifferencegrpby2 = df_check_table_source.groupby(list(df_check_table_source.columns))
                idx2 = [x[0] for x in dfDifferencegrpby2.groups.values() if len(x) == 2]
                df_check_table_source = df_check_table_source.reindex(idx2)
                df_check_table_source.insert(loc=0, column='Table_name', value=st)
        
                # print("Difference in source df",df_check_table_source)

                df_check_table_target = pd.concat([df4_copy, dfDifference1], ignore_index=True)
                dfDifferencegrpby3 = df_check_table_target.groupby(list(df_check_table_target.columns))
                idx3 = [x[0] for x in dfDifferencegrpby3.groups.values() if len(x) == 2]
                df_check_table_target = df_check_table_target.reindex(idx3)
                df_check_table_target.insert(loc=0, column='Table_name', value=tt)

                # print("DIfference in target df", df_check_table_target)

                df_final_output = df_check_table_source.append(df_check_table_target,ignore_index=True)
                # print("final data frame to return",df_final_output)


            logger.info("I am not in if /n/n")
    
    
            logger.info(df_final_output)
            logger.info(df_final_output)
            return st, df_final_output, error_dict

        except Exception as e:
            logger.exception(e)
            error_dict["error"]="exception occured during constraint check"
            return st, df_final_output, error_dict
   

        
        

    ## CASE 4: TO CHECK DATATYPE OF TWO COLUMNS FROM TWO DIFF TABLES 

    def datatype_check(self, st,tt):
        error_dict={}
        df_datatype_diff = pd.DataFrame()
        try:
            dfSourceTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            dfTargetTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)

            
            dict_diff_datatype={}
            dfSourceTable = pd.read_sql(qt.DATA_TYPE1_CHECK.format(st), self.source_engine)
            # print("dfSourceTable",dfSourceTable)
            logger.info(dfSourceTable)

            dfTargetTable = pd.read_sql(qt.DATA_TYPE1_CHECK.format(tt), self.target_engine)
            # print("dfTargetTable",dfTargetTable)

# //////////////////////////////////////////////////////////

            #  Uncomment Below >
            # dfSourceTable = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            # print("dfSourceTable",dfSourceTable)
            # logger.info(dfSourceTable)

            # dfTargetTable = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)
            # print("dfTargetTable",dfTargetTable)
         
            # listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]
            # print("listIdenticalCol", listIdenticalCol)
            # dfSourceCopy = dfSourceTable[listIdenticalCol].copy()
            # dfTargetCopy = dfTargetTable[listIdenticalCol].copy()
            # print("Source dataframe COLUMN", list(dfSourceCopy.columns))
            # print("TARGET dataframe COLUMN", list(dfTargetCopy.columns))

            # dtype_src = dfSourceCopy.dtypes.to_dict('records')
            # print("type src dict w/o astype",dtype_src)
            # # print("TARGET DTAFRAME", dfTargetCopy)

            # dict_src_col_type = dfSourceCopy.dtypes.astype(str).to_dict()
            # print("dict_src_col_type",dict_src_col_type)
            # dict_target_col_type = dfTargetCopy.dtypes.astype(str).to_dict()
            # print("dict_target_col_type",dict_target_col_type)
            # logger.info(dict_target_col_type)
            # logger.info("here")

# ///////////////////////////////////////////////////////////////

            SRC_DATA_DICT=dfSourceTable.to_dict('split')
            src_temp=SRC_DATA_DICT["data"]
            j=len(src_temp)
            src_dict={}
            for i in range(0,j):
                src_dict[src_temp[i][0]]={'data_type':src_temp[i][1],'data_length':src_temp[i][2]}
            # print("\n")
            # print("src_dict",src_dict)
            ########For Target Dict
            TRG_DATA_DICT=dfTargetTable.to_dict('split')
            trg_temp=TRG_DATA_DICT["data"]
            j=len(trg_temp)
            trg_dict={}
            for i in range(0,j):
                trg_dict[trg_temp[i][0]]={'data_type':trg_temp[i][1],'data_length':trg_temp[i][2]}
            print("\n")
            # print("trg_dict",trg_dict)
            print("\n")
            
            # print("here")
            # print(trg_dict)
            # for key,value in src_dict.items():
            #     print(src_dict[key],trg_dict[key])
            #     if src_dict[key] !=trg_dict[key]:
            #         dict_diff_datatype[key] = value
            #         # dict_diff_datatype[key] = trg_dict[key]
            # print("differnce",dict_diff_datatype)

            # New Update
            
            for key, value in src_dict.items():
                if key in trg_dict:
                    target_datatype = trg_dict[key]
                    # print("target_datatype",target_datatype)
                    if value['data_type'] == target_datatype['data_type'] and value['data_length'] == target_datatype['data_length']:
                        pass
                    else:
                        dict_diff_datatype[key] = target_datatype
                else:
                    dict_diff_datatype[key] = None

            # print("dict_diff_datatype",dict_diff_datatype)
            if len(dict_diff_datatype)!=0:
                logger.info("hey")
                df_datatype_diff = df_datatype_diff.append(dict_diff_datatype, ignore_index=True)
                df_datatype_diff.insert(loc=0,column='Table_name',value=tt)


# ////////////////////////////////////////////////////
            # for key,value in dict_target_col_type.items():
            #     print("///////////////////")
            #     print("key",key + "value",value)
            #     if dict_src_col_type[key] !=dict_target_col_type[key]:
            #         dict_diff_datatype[key] = value

            # if len(dict_diff_datatype)!=0:
            #     logger.info("hey")
            #     df_datatype_diff = df_datatype_diff.append(dict_diff_datatype, ignore_index=True)
            #     df_datatype_diff.insert(loc=0,column='Table_name',value=tt)

            return st,df_datatype_diff,error_dict
            # /////////////////////////////////////////// Above is original///////////
        except Exception as e:
            print("Exception",str(e))
            error_dict["error"] = "exception occured in data type check"   
            logger.error(error_dict["error"])
            return st,df_datatype_diff,error_dict  
        



    
#  Case 5: Trigger Validation

    def trigger_check(self, st,tt):
        # print("trigger check here")
        error_dict={}
        try:
            dfSourceTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            dfTargetTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)
            # print("dfSourceTable_check exist",dfSourceTable_check + "dfTargetTable_check",dfTargetTable_check)
            
            dfSourceTable = pd.read_sql(qt.Trigger1_check.format(st), self.source_engine)
            dfTargetTable = pd.read_sql(qt.Trigger1_check.format(tt), self.target_engine)
            logger.info(dfSourceTable)
            logger.info(dfTargetTable)
            listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]
            dfSourceCopy = dfSourceTable[listIdenticalCol].copy()
            dfTargetCopy = dfTargetTable[listIdenticalCol].copy()
            dfDifference = pd.concat([dfSourceCopy,dfTargetCopy],sort=True)
            dfDifference = dfDifference[listIdenticalCol]
            dfDifference = dfDifference.reset_index(drop=True)
            dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
            idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
            dfDifference=dfDifference.reindex(idx)
            logger.info(dfDifference)
            return st,dfDifference,error_dict

        except Exception as e: 
            
            dfDifference=pd.DataFrame()
            logger.error(e)
            error_dict['error'] = "exception occured in Trigger_check"
            return st,dfDifference,error_dict
            # print("inside error",str(e))
        
        
            

#  Case 6: Sequence Validation

    def Sequence_check(self, st,tt):

        error_dict={}
        try:
            dfSourceTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)
            dfTargetTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)
            # print("dfSourceTable_check exist",dfSourceTable_check + "dfTargetTable_check",dfTargetTable_check)

            dfSourceTable = pd.read_sql(qt.Sequence1_check.format(st), self.source_engine)
            logger.info(dfSourceTable)
            dfTargetTable = pd.read_sql(qt.Sequence1_check.format(tt), self.target_engine)
            logger.info(dfTargetTable)
            listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]
            # print("listIdenticalCol",listIdenticalCol)
            dfSourceCopy = dfSourceTable[listIdenticalCol].copy()
            dfTargetCopy = dfTargetTable[listIdenticalCol].copy()
            # print("TARGET dataframe COLUMN", list(dfTargetCopy.columns))
            # print("TARGET DTAFRAME",dfTargetCopy)
            dfDifference = pd.concat([dfSourceCopy,dfTargetCopy],sort=True)
            # print("differnece dataframe column", list(dfDifference.columns))
            dfDifference = dfDifference[listIdenticalCol]
            dfDifference = dfDifference.reset_index(drop=True)
            dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
            idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
            dfDifference=dfDifference.reindex(idx)
            return st,dfDifference,error_dict
        except Exception as e:
            dfDifference=pd.DataFrame()
            logger.error(e)
            error_dict['error'] = "exception occured in Sequence_check"
            return st,dfDifference,error_dict


#  Case 7: Sequence Validation

    def index_check(self, st,tt):
        error_dict={}
        try:
            # print("in index")
            dfSourceTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(st), self.source_engine)      
            dfTargetTable_check = pd.read_sql(qt.GET_ALL_DATA_QUERY.format(tt), self.target_engine)
            # print("dfSourceTable_check exist",dfSourceTable_check + "dfTargetTable_check",dfTargetTable_check)
            dfSourceTable = pd.read_sql(qt.Index1_check.format(st), self.source_engine)
            # print("dfSourceTable",dfSourceTable)
            logger.info(dfSourceTable)
            dfTargetTable = pd.read_sql(qt.Index1_check.format(tt), self.target_engine)
            # print("dfSourceTable",dfTargetTable)
            logger.info(dfTargetTable)
            listIdenticalCol = [col for col in list(dfSourceTable.columns) if col in list(dfTargetTable.columns)]
            # print("listIdenticalCol",listIdenticalCol)
            dfSourceCopy = dfSourceTable[listIdenticalCol].copy()
            dfTargetCopy = dfTargetTable[listIdenticalCol].copy()
            # print("Hi")
            dfDifference = pd.concat([dfSourceCopy,dfTargetCopy],sort=True)
            dfDifference = dfDifference[listIdenticalCol]
            dfDifference = dfDifference.reset_index(drop=True)
            dfDifferencegrpby = dfDifference.groupby(list(dfDifference.columns))
            idx = [x[0] for x in dfDifferencegrpby.groups.values() if len(x) == 1]
            dfDifference=dfDifference.reindex(idx)
            # print("DIFFERENCE in INDX",dfDifference)
            return st,dfDifference,error_dict
        except Exception as e:
            logger.error(e)
            dfDifference=pd.DataFrame()
            error_dict['error'] = "exception occured in Index_check"
            return st,dfDifference,error_dict
 







