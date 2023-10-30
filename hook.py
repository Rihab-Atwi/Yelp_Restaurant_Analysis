from db_handler import *
from logging_handler import * 
from lookups import *
import os
from pandas_handler import *
from misc_handler import *
import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

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
            {ETL_Checkpoint.FISRT_COLUMN.value} TIMESTAMP,
            {ETL_Checkpoint.SECOND_COLUMN.value} INT

        );
        """
        execute_query(db_session, query)
    except Exception as e:
        show_error_message(HookSteps.CREATE_ETL_CHECKPOINT.value,str(e))
    finally:
        return query

def insert_or_update_etl_checkpoint(db_session, etl_time_exists=False, etl_id_exists=False,
                                    target_schema=DestinationSchemaName.Datawarehouse,
                                    table_name=ETL_Checkpoint.TABLE,
                                    column_name_1=ETL_Checkpoint.FISRT_COLUMN,
                                    column_name_2=ETL_Checkpoint.SECOND_COLUMN,
                                    Staging_table=StagingTablesNamesWithID.business):
    success_indicator = False
    error_message = None
    try:
        if etl_time_exists and etl_id_exists:
            update_query = f"""
                UPDATE {target_schema.value}.{table_name.value}
                SET {column_name_1.value} = '{datetime.datetime.now()}',
                    {column_name_2.value} = (SELECT MAX({DataDate.ID.value}) FROM {target_schema.value}.{Staging_table.value})
            """
            execute_query(db_session=db_session, query=update_query)
            success_indicator = True
        else:
            insert_query = f"""
                INSERT INTO {target_schema.value}.{table_name.value}
                VALUES('{ETL_Checkpoint.ETL_DEFAULT_DATE.value}','{ETL_Checkpoint.ETL_DEFAULT_ID.value}')
            """
            execute_query(db_session=db_session, query=insert_query)
            success_indicator = True
    except Exception as e:
        error_message = str(e)
    return success_indicator, error_message


def return_etl_last_updated_date(db_session,
                                target_schema = DestinationSchemaName.Datawarehouse,
                                etl_date = ETL_Checkpoint.ETL_DEFAULT_DATE,
                                etl_id = ETL_Checkpoint.ETL_DEFAULT_ID,
                                table_name = ETL_Checkpoint.TABLE,
                                column_name_1 = ETL_Checkpoint.FISRT_COLUMN,
                                column_name_2 = ETL_Checkpoint.SECOND_COLUMN):
    etl_time_exists = False
    etl_id_exists = False
    return_date = None
    try:
        query = f"SELECT {column_name_1.value},{column_name_2.value}  FROM {target_schema.value}.{table_name.value} ORDER BY {column_name_1.value} DESC LIMIT 1"
        etl_df = read_data_as_dataframe(file_type = InputTypes.SQL, file_path = query, db_session= db_session)
        if len(etl_df) == 0:
            return_date, return_id = etl_date.value, etl_id.value
        else:
            return_date = etl_df[column_name_1.value].iloc[0]
            return_id = etl_df[column_name_2.value].iloc[0]
            etl_time_exists = True
            etl_id_exists = True
    except Exception as e:
        show_error_message(HookSteps.RETURN_LAST_ETL_RUN.value, str(e))
    finally:    
        return return_date,return_id, etl_time_exists,etl_id_exists
    
    
def insert_into_staging_tables(db_session, target_schema=DestinationSchemaName.Datawarehouse, etl_date=None, etl_id=None):
    staging = {}
    try:
        all_data = cleaned_dataframes_dict()
        
        # Process tables with ID
        data_frame_names = [member.name for member in StagingTablesNamesWithID]
        tables_names = [member.value for member in StagingTablesNamesWithID]
        
        for data_frame_name, table_name in zip(data_frame_names, tables_names):
            staging_all_data = all_data.get(data_frame_name, None)
            if staging_all_data is not None:
                staging_all_data = staging_all_data[staging_all_data[DataDate.ID.value] > etl_id]

                if not staging_all_data.empty:
                    insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name)
                    execute_return = execute_query(db_session=db_session, query=insert_stmt)

                    if execute_return != ErrorHandling.NO_ERROR:
                        raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: Error executing insert_stmt for table '{table_name}'.")

                    staging[data_frame_name] = f"Inserted new data after '{etl_id}' into {table_name} successfully."

        # Process tables with Date 
        data_frame_names = [member.name for member in StagingTablesNamesWithDate]
        tables_names = [member.value for member in StagingTablesNamesWithDate]

        for data_frame_name, table_name in zip(data_frame_names, tables_names):
            staging_all_data = all_data.get(data_frame_name, None)
            if staging_all_data is not None:
                staging_all_data = staging_all_data[staging_all_data[DataDate.DATE.value] > etl_date]

                if not staging_all_data.empty:
                    if data_frame_name == 'review':
                        text_column_name = 'text'  
                        text_data = staging_all_data[text_column_name]

                        analyzer = SentimentIntensityAnalyzer()

                        def update_sentiment_info(text):
                            sentiment = analyzer.polarity_scores(text)
                            sentiment_score = sentiment['compound']

                            if sentiment_score > 0.05:
                                sentiment_label = 'Positive'
                            elif sentiment_score < -0.05:
                                sentiment_label = 'Negative'
                            else:
                                sentiment_label = 'Neutral'
                                
                            return sentiment_score, sentiment_label

                        sentiment_info = text_data.apply(update_sentiment_info)
                        staging_all_data['score'], staging_all_data['sentiment'] = zip(*sentiment_info)

                    insert_stmt = insert_into_sql_statement_from_df(staging_all_data, target_schema.value, table_name)
                    execute_return = execute_query(db_session=db_session, query=insert_stmt)

                    if execute_return != ErrorHandling.NO_ERROR:
                        raise Exception(f"{HookSteps.INSERT_INTO_STG_TABLE.value}: Error executing insert_stmt for table '{table_name}'.")

                    staging[data_frame_name] = f"Inserted new data after '{etl_date}' into {table_name} successfully."
    
    except Exception as e:
        show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
    
    finally:
        return staging
   
   
def execute_hook(logger):
    step = None
    logger.info("Hook:")
    try:
        step=1
        logger.info("Step 1: Create a database connection")
        db_session = create_connection()

        step=2
        logger.info("Step 2: Create ETL checkpoint")
        create_etl_checkpoint(DestinationSchemaName.Datawarehouse, db_session)

        step = 3
        logger.info("Step 3: Retrieve ETL last updated date")
        etl_date,etl_id, etl_time_exists,etl_id_exists = return_etl_last_updated_date(db_session)

        step=4
        logger.info("Step 4: Inserte data into staging tables")
        insert_into_staging_tables(db_session, DestinationSchemaName.Datawarehouse, etl_date,etl_id)

        step=5
        logger.info("Step 5: Execute SQL folder hook")
        execute_sql_folder_hook(db_session)

        step=6
        logger.info("Step 6: Inserte or updating ETL checkpoint")
        insert_or_update_etl_checkpoint(db_session, etl_time_exists=etl_time_exists,etl_id_exists=etl_id_exists)

        step=7
        logger.info("Step 7: Close the database connection\n")
        close_connection(db_session)
    except Exception as e:
        error_prefix = ErrorHandling.HOOK_SQL_ERROR.value
        show_error_message(error_prefix, str(e))

