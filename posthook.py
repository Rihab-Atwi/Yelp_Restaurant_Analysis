from db_handler import execute_query, create_connection, close_connection
from lookups import *
from logging_handler import show_error_message


def truncate_staging_table(db_session, source_name=DestinationSchemaName.Datawarehouse):
    try:
        tables_names = [member.value for member in StagingTablesNamesWithID]
        for table_name in tables_names:
            dst_table = f"{source_name.value}.{table_name}"
            truncate_query = f"""
                TRUNCATE TABLE {dst_table}
            """
            execute_query(db_session, truncate_query)
            print(f"Table {dst_table} truncated successfully.")
        tables_names = [member.value for member in StagingTablesNamesWithDate]
        for table_name in tables_names:
            dst_table = f"{source_name.value}.{table_name}"
            truncate_query = f"""
                TRUNCATE TABLE {dst_table}
            """
            execute_query(db_session, truncate_query)
            print(f"Table {dst_table} truncated successfully.")
    except Exception as e:
        show_error_message(ErrorHandling.TRUNCATE_ERROR.value, str(e))

def execute_posthook():
    try:
        db_session = create_connection()
        truncate_staging_table(db_session, source_name = DestinationSchemaName.Datawarehouse)
        close_connection(db_session)
    except Exception as e:
        show_error_message(ErrorHandling.EXECUTE_POSTHOOK_ERROR.value, str(e))



# def insert_into_staging_tables(db_session, target_schema=DestinationSchemaName.Datawarehouse, etl_date=None, etl_id = None):
#     staging = {}
#     try:
#         all_data = cleaned_dataframes_dict()
#         data_frame_names = [member.name for member in StagingTablesNamesWithID]
#         tables_names = [member.value for member in StagingTablesNamesWithID]
        
#         for data_frame_name, table_name in zip(data_frame_names, tables_names):
#             staging_all_data = all_data[data_frame_name]
#             staging_all_data = staging_all_data[staging_all_data[DataDate.ID.value] > etl_id]

#             if len(staging_all_data) > 0:
#                 insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name)
#                 execute_return = execute_query(db_session=db_session, query=insert_stmt)

#                 if execute_return != ErrorHandling.NO_ERROR:
#                     raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: error executing insert_stmt.")

#                 staging[data_frame_name] = f"Inserted new data after '{etl_id}' into {table_name} successfully."
        
#         data_frame_names = [member.name for member in StagingTablesNamesWithDate]
#         tables_names = [member.value for member in StagingTablesNamesWithID]
        
#         for data_frame_name, table_name in zip(data_frame_names, tables_names):
#             staging_all_data = all_data[data_frame_name]
#             staging_all_data = staging_all_data[staging_all_data[DataDate.DATE.value] > etl_date]

#             if len(staging_all_data) > 0:
#                 insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name)
#                 execute_return = execute_query(db_session=db_session, query=insert_stmt)

#                 if execute_return != ErrorHandling.NO_ERROR:
#                     raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: error executing insert_stmt.")

#                 staging[data_frame_name] = f"Inserted new data after '{etl_date}' into {table_name} successfully."
    
#     except Exception as e:
#         show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
    
#     finally:
#         return staging

