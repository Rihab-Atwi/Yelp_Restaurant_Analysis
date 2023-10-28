from db_handler import execute_query, create_statement_from_df, create_connection, close_connection
from logging_handler import show_error_message 
from lookups import ErrorHandling, PreHookSteps, SQLCommandsPath, DestinationSchemaName
import os
from pandas_handler import cleaned_dataframes_dict
from misc_handler import get_sql_files_list


def execute_sql_folder_prehook(db_session, target_schema = DestinationSchemaName.Datawarehouse, sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = None
    try:
        sql_files = get_sql_files_list(sql_commands_path)
        for file in sql_files:
            if '_prehook' in file:
                with open(os.path.join(sql_commands_path.value,file), 'r') as f:
                    sql_query = f.read()
                    sql_query = sql_query.replace('target_schema', target_schema.value)
                    return_val = execute_query(db_session= db_session, query= sql_query)
                    if not return_val == ErrorHandling.NO_ERROR:
                        raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(file))
    except Exception as e:
        show_error_message(ErrorHandling.PREHOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files
    
def create_sql_staging_table(db_session, target_schema):
    dataframes_dict =cleaned_dataframes_dict()
    create_statments = {}
    for table_name, source_df in dataframes_dict.items():
        if source_df.empty:
            raise Exception(f"No DataFrame returned for table '{table_name}'")
        try:
            stg_table_name=f"stg_{table_name}"
            staging_df = source_df.head(1)
            columns = list(staging_df.columns)
            create_stmt = create_statement_from_df(staging_df, f"{target_schema.value}", stg_table_name)
            execute_return_val = execute_query(db_session=db_session, query=create_stmt)
            if execute_return_val != ErrorHandling.NO_ERROR:
                raise Exception(f"{ErrorHandling.EXECUTE_QUERY_ERROR}: error occurred during table creation.")
            create_sql_stg_table_idx(db_session, f"{target_schema.value}", stg_table_name, columns[0])
            create_statments[table_name] =  create_stmt
        except Exception as e:
            show_error_message(ErrorHandling.ERROR_CREATING_STAGING_TABLE.value, str(e))
    return create_statments

def create_sql_stg_table_idx(db_session,source_name,table_name,index_val):
    try:
        query = f"CREATE INDEX IF NOT EXISTS idx_{table_name} ON {source_name}.{table_name} ({index_val});"
        execute_query(db_session,query)
    except Exception as e:
        show_error_message(PreHookSteps.CREATE_TABLE_IDX.value, str(e))

def execute_prehook(sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    step = None
    try:
        db_session = create_connection()
        execute_sql_folder_prehook(db_session,DestinationSchemaName.Datawarehouse,sql_commands_path)
        create_sql_staging_table(db_session,DestinationSchemaName.Datawarehouse)
        close_connection(db_session)
    except Exception as e:
        error_prefix = f'{ErrorHandling.PREHOOK_SQL_ERROR.value} on step {step}'
        show_error_message(error_prefix,str(e))
