import sqlalchemy
from sqlalchemy import create_engine

def validate_connection(connection_details):
    # print (connection_details)
    try:
        if connection_details:
            if connection_details["database_type"] == "oracle":
                conn_str = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(connection_details["username"],
                                                                    connection_details["password"],
                                                                    connection_details["hostname"],
                                                                    connection_details["port"],
                                                                    connection_details["database"])
                # print(conn_str)
                engine = create_engine(conn_str)
                connection = engine.connect()
                # print(engine)

                if connection:
                    return (True,None,conn_str)
                    # return (True,None)
                else:
                    print("Source Connection Failed")
                    return (False, None, None)
    
            if connection_details["database_type"] == "oracle":
                conn_str_1 = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(connection_details["username"],
                                                                    connection_details["password"],
                                                                    connection_details["hostname"],
                                                                    connection_details["port"],
                                                                    connection_details["database"])
                # print(conn_str_1)
                engine_1 = create_engine(conn_str_1)
                connection_1  = engine_1.connect()
                # print(engine_1)
                if connection_1:
                    # return (True,None)
                    return (True,None,conn_str_1)
                else:
                    print("Target connection Failed")
                    return (False, None, None)
           
             
    except Exception as e:
        return (False, e,None)
            

