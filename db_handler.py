import psycopg2
from lookups import ErrorHandling, InputTypes
from logging_handler import show_error_message
import pandas as pd
from datetime import time, datetime
import warnings

warnings.simplefilter(action='ignore', category=UserWarning)

db_name = "yelp_data"
db_user = "postgres"
db_pass = "admin"
db_host = "localhost"
db_port = 5432

def create_connection():
    db_session = None
    try:
        db_session = psycopg2.connect(
            database = db_name, 
            user = db_user, 
            password = db_pass, 
            host = db_host, 
            port = db_port
        )
        print("Connection is successful.")
    except Exception as e:
        error_prefix = ErrorHandling.ERROR_CONNECTING_TO_DB.value
        error_suffix = str(e)
        show_error_message(error_prefix, error_suffix)
    finally:
        return db_session

def return_query(db_session,query):
    results = None
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        db_session.commit()
    except Exception as e:
        error_prefix = ErrorHandling.DB_RETURN_QUERY_ERROR.value
        error_suffix = str(e)
        show_error_message(error_prefix, error_suffix)
    finally:
        return results

def read_data_as_dataframe(file_type, file_path, db_session = None):
    return_dataframe = None
    try:
        if file_type == InputTypes.CSV:
            return_dataframe = pd.read_csv(file_path)
        elif file_type == InputTypes.EXCEL:
            return_dataframe = pd.read_excel(file_path)
        elif file_type == InputTypes.SQL:
            return_dataframe = pd.read_sql_query(con= db_session, sql= file_path)
        else:
            raise Exception("The file type does not exist, please check input")
        
    except Exception as e:
        suffix = str(e)
        if file_type == InputTypes.CSV:
            error_prefix = ErrorHandling.RETURN_DATA_CSV_ERROR.value
        elif file_type == InputTypes.EXCEL:
            error_prefix = ErrorHandling.RETURN_DATA_EXCEL_ERROR.value
        elif file_type == InputTypes.SQL:
            error_prefix = ErrorHandling.RETURN_DATA_SQL_ERROR.value
        else:
            error_prefix = ErrorHandling.RETURN_DATA_UNDEFINED_ERROR.value
        show_error_message(error_prefix, suffix)

    finally:
        return return_dataframe
    
def execute_query(db_session, query):
    return_val = ErrorHandling.NO_ERROR
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()
    except Exception as e:
        error_prefix = ErrorHandling.EXECUTE_QUERY_ERROR
        return_val = error_prefix
        error_suffix = str(e)
        show_error_message(error_prefix.value, error_suffix)
    finally:
        if cursor:
            cursor.close()
        return return_val

def create_statement_from_df(df, schema_name, table_name):
    create_table_stmt = None 
    try:
        data_type_mapping = {
            'int64' : 'INTEGER', 
            'float64' : 'FLOAT', 
            'bool' : 'BOOLEAN', 
            'object' : 'TEXT', 
            'datetime64[ns]' : 'TIMESTAMP'
        }
        cols = [f"{column} {data_type_mapping.get(str(dtype), 'TEXT')}" for column, dtype in df.dtypes.items()]
        create_table_stmt = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
        create_table_stmt += ", \n".join(cols)
        create_table_stmt += "\n);"
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_CREATE_STMNT.value, str(e))
    finally:
        return create_table_stmt

    
def insert_into_sql_statement_from_df(df, schema_name, table_name):
    insert_statement = None
    try:
        column_names = ', '.join(df.columns)
        values_list = []
        for _, row in df.iterrows():
            values_strs = []
            for val in row.values:
                if isinstance(val, str):
                    val_escaped = val.replace("'", "''")
                    values_strs.append(f"'{val_escaped}'")
                elif isinstance(val, datetime):
                    values_strs.append(f"'{val}'")
                elif pd.isna(val):
                    values_strs.append("NULL")
                else:
                    values_strs.append(str(val))
            values = ', '.join(values_strs)
            values_list.append(f"({values})")
        values_str = ',\n'.join(values_list)
        insert_statement = f"INSERT INTO {schema_name}.{table_name} ({column_names}) VALUES\n{values_str};"
    except Exception as e:
        show_error_message(error_prefix = ErrorHandling.ERROR_INSERT_STMNT, error_suffix= str(e))
    finally:
        return insert_statement

def refresh_connection(db_session):
    db_session.close()
    time.sleep(5)
    db_session = create_connection()
    return db_session

def close_connection(db_session):
    return_val = None
    try:
        return_val = db_session.close()
        print("Connection is closed.")
    except Exception as e:
        show_error_message(ErrorHandling.ERROR_CLOSING_CONN.value,str(e))
    finally:
        return return_val