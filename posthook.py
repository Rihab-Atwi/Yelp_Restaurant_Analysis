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