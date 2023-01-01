# import Tables as tb


# def test_fun():
#     print "My function"

DATABASE_CONNECTION = 'oracle://faizal:abc@localhost:1521/orcl'

MINUS_COMPARISION_QUERY = "select * from {1} minus select * from {0} "

GET_ALL_DATA_QUERY = "select * from {0}"

ROW_COUNT_CHECK = "SELECT(SELECT COUNT(*) FROM {0}) AS number_of_records_source from dual" 
ROW_COUNT_CHECK1 = "SELECT(SELECT COUNT(*) FROM {0}) AS number_of_records_target from dual"

DATA_TYPE1_CHECK = "SELECT column_name, data_type, data_length FROM all_tab_columns where table_name = '{0}'"
DATA_TYPE2_CHECK = "SELECT table_name, column_name, data_type, data_length FROM all_tab_columns where table_name = '{1}'"

CON1_CHECK = " SELECT ac.table_name,column_name,ac.constraint_name,DECODE (constraint_type, 'P', 'Primary Key','R', 'Foreign Key','Not Null') key_type, (SELECT ac2.table_name FROM all_constraints ac2 WHERE AC2.CONSTRAINT_NAME = AC.R_CONSTRAINT_NAME) fK_to_table FROM all_cons_columns acc, all_constraints ac WHERE acc.constraint_name = ac.constraint_name  AND acc.table_name = ac.table_name AND CONSTRAINT_TYPE IN ('P', 'R', 'C') AND ac.table_name = '{0}' ORDER BY table_name, constraint_type, position"
# CON1_CHECK=" SELECT ac.table_name,column_name,ac.constraint_name,DECODE (constraint_type, 'P', 'Primary Key','R', 'Foreign Key','Not Null') key_type, (SELECT ac2.table_name FROM all_constraints ac2 WHERE AC2.CONSTRAINT_NAME = AC.R_CONSTRAINT_NAME) fK_to_table FROM all_cons_columns acc, all_constraints ac WHERE acc.constraint_name = ac.constraint_name  AND acc.table_name = ac.table_name AND CONSTRAINT_TYPE IN ('P', 'R','C') AND ac.owner||'.'||ac.table_name = '{0}' ORDER BY table_name, constraint_type, position" 

Trigger1_check = "SELECT TRIGGER_NAME,TRIGGER_TYPE,TRIGGERING_EVENT,ACTION_TYPE,STATUS from all_triggers where table_name = '{0}'"
# Trigger2_check = "SELECT OWNER,TABLE_NAME,COLUMN_NAME,TRIGGER_NAME,TRIGGER_TYPE,TRIGGERING_EVENT,ACTION_TYPE,TRIGGER_BODY,STATUS from all_triggers where table_name = '{1}'"

# Index1_check = "SELECT TABLE_NAME, index_name, COLUMN_NAME from all_ind_columns where  table_name = '{0}'"
Index1_check = "SELECT IND.INDEX_TYPE,IND.UNIQUENESS,LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position) from all_ind_columns COL,all_indexes IND where COL.INDEX_OWNER=IND.OWNER and COL.INDEX_NAME=IND.INDEX_NAME and COL.table_name = '{0}' group by IND.INDEX_TYPE,IND.UNIQUENESS,COL.INDEX_NAME"

Sequence1_check = "SELECT sequence_name, min_value,max_value , increment_by  FROM all_sequences x,all_tables B WHERE x.sequence_owner=B.owner AND B.TABLE_NAME='{0}'"
# Sequence2_check = "SELECT table_name, sequence_owner as owner, sequence_name, min_value,max_value , increment_by  FROM all_sequences x,all_tables B WHERE x.sequence_owner=B.owner AND B.TABLE_NAME='{1}'"


#  If user is using schema.Table_name , add COL.table_owner||'.'||COL.table_name = '{0}' to each query

# SELECT TRIGGER_NAME,COLUMN_NAME,TRIGGER_TYPE,TRIGGERING_EVENT,ACTION_TYPE,STATUS from all_triggers where owner||'.'||table_name = '{0}' 
# SELECT  SEQUENCE_NAME,min_value,max_value , increment_by  FROM all_sequences x,all_tables B WHERE x.sequence_owner=B.owner AND B.TABLE_NAME='{0}' 