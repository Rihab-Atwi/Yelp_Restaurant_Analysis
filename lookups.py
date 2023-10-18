from enum import Enum

class ErrorHandling(Enum):
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    NO_ERROR = "No Errors"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    RETURN_DATA_CSV_ERROR = "CSV Error"
    RETURN_DATA_EXCEL_ERROR = "Excel Error"
    RETURN_DATA_SQL_ERROR = "SQL Error"
    RETURN_DATA_UNDEFINED_ERROR = "Undefined Error"
    ERROR_CONNECTING_TO_DB = "Error connecting to this database!"
    ERROR_CLOSING_CONN = "Error closing connection, check if session exists"
    ERROR_INSERT_STMNT = "Error in insert statment"
    ERROR_CREATE_STMNT = "Error in create statment"
    ERROR_BUSINESS_CLEANING = "Error in cleaning business dataframe"
    ERROR_ATTRIBUTES_CLEANING = "Error in cleaning attributes dataframe"
    ERROR_CHECKIN_CLEANING = "Error in cleaning checkin dataframe"
    ERROR_USER_CLEANING = "Error in cleaning user dataframe"
    ERROR_ELITE_user_CLEANING = "Error in cleaning elite user dataframe"
    ERROR_DICT = "Error in clenaning all dataframes and create a dict"
    PREHOOK_SQL_ERROR = "Prehook error"
    ERROR_CREATING_STAGING_TABLE = "Error creating staging table"
    HOOK_SQL_ERROR = "Error in executing the SQL commands of the hook (error in the hook step)"

class InputTypes(Enum):
    CSV = "csv"
    EXCEL = "excel"
    SQL = "sql"
    TXT = "txt"

class SourceFiles(Enum):
    BUSINESS = "./CSV_files/business.csv"
    CHECKIN = "./CSV_files/checkin.csv"
    REVIEW = "./CSV_files/review.csv"
    USER = "./CSV_files/user.csv"

class DestinationSchemaName(Enum):
    Datawarehouse = "dw_reporting"

class SQLCommandsPath(Enum):
    SQL_FOLDER = "./SQL_Commands/"
    
class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder_prehook"
    CREATE_TABLE_IDX = "create_sql_stg_table_idx"
class HookSteps(Enum):
    CREATE_ETL_CHECKPOINT = "Create_etl_checkpoint"
    EXECUTE_SQL_QUERY = "Execute_sql_folder_hook"
    INSERT_UPDATE_ETL_CHECKPOINT = "Insert_or_update_etl_checkpoint"
    RETURN_LAST_ETL_RUN = "Return_etl_last_updated_date"
    INSERT_INTO_STG_TABLE = "Insert_into_stg_tables"

class ETL_Checkpoint(Enum):
    TABLE = "etl_checkpoint"
    FISRT_COLUMN = "etl_last_run"
    SECOND_COLUMN = "etl_last_id"
    ETL_DEFAULT_DATE = "1900-01-01 00:00:00"
    ETL_DEFAULT_ID = -1

class DataDate(Enum):
    DATE = 'date'
    ID = "numeric_id"

class StagingTablesNamesWithDate(Enum):
    checkin = "stg_checkin"
    user = "stg_user"
    elite_user = "stg_elite_user"
    review = "stg_review"

class StagingTablesNamesWithID(Enum):
    business = "stg_business"
    attributes = "stg_attributes"
