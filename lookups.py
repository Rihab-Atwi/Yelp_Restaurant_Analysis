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
    EXECUTE_SQL_QUERY = "Execute SQL folder prehook"