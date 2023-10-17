from db_handler import *
from logging_handler import show_error_message  
from lookups import *
import os
from pandas_handler import *
from misc_handler import get_sql_files_list
import datetime

def execute_sql_folder_hook(db_session, target_schema = DestinationSchemaName.Datawarehouse, sql_commands_path = SQLCommandsPath.SQL_FOLDER):
    sql_files = None
    try:
        sql_files = get_sql_files_list(sql_commands_path)
        for file in sql_files:
            if '_hook' in file:
                with open(os.path.join(sql_commands_path.value,file), 'r') as f:
                    sql_query = f.read()
                    sql_query = sql_query.replace('target_schema', target_schema.value)
                    return_val = execute_query(db_session= db_session, query= sql_query)
                    if not return_val == ErrorHandling.NO_ERROR:
                        raise Exception(f"{HookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(file))
    except Exception as e:
        show_error_message(ErrorHandling.HOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files
    
def create_etl_checkpoint(target_schema, db_session):
    query = None
    try:
        query = f"""CREATE TABLE IF NOT EXISTS {target_schema.value}.{ETL_Checkpoint.TABLE.value}
        (
            {ETL_Checkpoint.COLUMN_1.value} TIMESTAMP

        );
        """
        execute_query(db_session, query)
    except Exception as e:
        show_error_message(HookSteps.CREATE_ETL_CHECKPOINT.value,str(e))
    finally:
        return query
    
def insert_or_update_etl_checkpoint(db_session,
                                    etl_time_exists = False,
                                    target_schema = DestinationSchemaName.Datawarehouse,
                                    table_name = ETL_Checkpoint.TABLE,
                                    column_name = ETL_Checkpoint.COLUMN_1):
    try: 
        if etl_time_exists:
            update_query = f"""
                UPDATE {target_schema.value}.{table_name.value}
                SET {column_name.value} = '{datetime.datetime.now()}'
            """
            execute_query(db_session=db_session, query=update_query)
        else:
            insert_query = f"""
                INSERT INTO {target_schema.value}.{table_name.value}
                VALUES('{ETL_Checkpoint.ETL_DEFAULT_DATE.value}')
            """
            execute_query(db_session=db_session, query=insert_query)
    except Exception as e:
        show_error_message(HookSteps.INSERT_UPDATE_ETL_CHECKPOINT.value,str(e))

def return_etl_last_updated_date(db_session,
                                target_schema = DestinationSchemaName.Datawarehouse,
                                etl_date = ETL_Checkpoint.ETL_DEFAULT_DATE,
                                table_name = ETL_Checkpoint.TABLE,
                                column_name = ETL_Checkpoint.COLUMN_1):
    etl_time_exists = False
    return_date = None
    try:
        query = f"SELECT {column_name.value} FROM {target_schema.value}.{table_name.value} ORDER BY {column_name.value} DESC LIMIT 1"
        etl_df = read_data_as_dataframe(file_type = InputTypes.SQL, file_path = query, db_session= db_session)
        if len(etl_df) == 0:
            return_date = etl_date.value
        else:
            return_date = etl_df[column_name.value].iloc[0]
            etl_time_exists = True
    except Exception as e:
        show_error_message(HookSteps.RETURN_LAST_ETL_RUN.value, str(e))
    finally:    
        return return_date, etl_time_exists
    
    
# def insert_into_stg_tables(db_session, target_schema = DestinationSchemaName.Datawarehouse, etl_date = None):
#     staging = None
#     selected_tables = [member.name for member in StagingTablesNamesWithDate] 
#     try:
#         all_data = read_data_as_dataframe(InputTypes.CSV, SourceFiles.REVIEW.value)
#         staging_all_data = all_data[all_data[DataDate.DATE.value] > etl_date]
#         if len(staging_all_data)>0:
#             insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, StagingTablesNames.STG_ALL_DATA.value)
#             execute_return = execute_query(db_session=db_session, query= insert_stmt)
#             if execute_return != ErrorHandling.NO_ERROR:
#                 raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: error executing insert_stmt.")
#             staging = f"Inserted new data after '{etl_date}' into staging table succesfully."
#     except Exception as e:
#         show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
#     finally:
#         return staging
    
# def insert_into_staging_tables(db_session, target_schema=DestinationSchemaName.Datawarehouse, etl_date=None):
#     staging = {}
#     try:
#         all_data = cleaned_dataframes_dict()
#         data_frames_names = [member.name for member in StagingTablesNamesWithDate]
#         for data_frame in data_frames_names:
#             if table_name in [member.value for member in StagingTablesNamesWithDate]:
#                 staging_all_data = all_data[data_frame]
#                 staging_all_data = staging_all_data[staging_all_data[DataDate.DATE.value] > etl_date]

#                 if len(staging_all_data) > 0:
#                     insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name.value)
#                     execute_return = execute_query(db_session=db_session, query=insert_stmt)

#                     if execute_return != ErrorHandling.NO_ERROR:
#                         raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: error executing insert_stmt.")

#                     staging[table_name.value] = f"Inserted new data after '{etl_date}' into {table_name.value} successfully."
#             else:
#                 raise ValueError("Invalid table name")
    
#     except Exception as e:
#         show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
    
#     finally:
#         return staging

def insert_into_staging_tables(db_session, target_schema=DestinationSchemaName.Datawarehouse, etl_date=None):
    staging = {}
    try:
        all_data = cleaned_dataframes_dict()
        data_frame_names = [member.name for member in StagingTablesNamesWithDate]
        tables_names = [member.value for member in StagingTablesNamesWithDate]
        
        for data_frame_name, table_name in zip(data_frame_names, tables_names):
            staging_all_data = all_data[data_frame_name]
            staging_all_data = staging_all_data[staging_all_data[DataDate.DATE.value] > etl_date]

            if len(staging_all_data) > 0:
                insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name)
                execute_return = execute_query(db_session=db_session, query=insert_stmt)

                if execute_return != ErrorHandling.NO_ERROR:
                    raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: error executing insert_stmt.")

                staging[data_frame_name] = f"Inserted new data after '{etl_date}' into {table_name} successfully."
    
    except Exception as e:
        show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
    
    finally:
        return staging


    
def execute_hook():
    step = None
    try:
        step = 1
        db_session = create_connection()
        step = 2
        create_etl_checkpoint(DestinationSchemaName.Datawarehouse, db_session)
        step = 3
        etl_date, etl_time_exists = return_etl_last_updated_date(db_session)
        step = 4
        insert_into_staging_tables(db_session, DestinationSchemaName.Datawarehouse, etl_date)
        step = 5
        execute_sql_folder_hook(db_session)
        step = 6
        insert_or_update_etl_checkpoint(db_session, etl_time_exists=etl_time_exists)
        step = 7
        close_connection(db_session)
    except Exception as e:
        error_prefix = f'{ErrorHandling.HOOK_SQL_ERROR.value} on step {step}'
        show_error_message(error_prefix, str(e))

