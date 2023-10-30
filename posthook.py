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
        tables_names = [member.value for member in StagingTablesNamesWithDate]
        for table_name in tables_names:
            dst_table = f"{source_name.value}.{table_name}"
            truncate_query = f"""
                TRUNCATE TABLE {dst_table}
            """
            execute_query(db_session, truncate_query)
    except Exception as e:
        show_error_message(ErrorHandling.TRUNCATE_ERROR.value, str(e))

def execute_posthook(logger):
    step = None
    logger.info("Posthook:")
    try:
        step = 1
        logger.info("Step 1: Create a database connection")
        db_session = create_connection()

        step=2
        logger.info("Step 2: Truncate staging table")
        truncate_staging_table(db_session, source_name = DestinationSchemaName.Datawarehouse)

        step=3
        logger.info("Step 3: Close database connection")
        close_connection(db_session)

    except Exception as e:
        show_error_message(ErrorHandling.EXECUTE_POSTHOOK_ERROR.value, str(e))