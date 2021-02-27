from enum import Enum


class TransformStageTypes(Enum) :
    FILE_READER = 'filereader'
    FILE_WRITER = 'filewriter'
    RDBMS_READER = 'rdbmsreader'
    RDBMS_WRITER = 'rdbmswriter'
    DATA_VALIDATION = 'validation'
    DATA_STANDARDIZATION = 'standardization'
    DATA_TRANSFORMATION = 'transformation'
    DATA_AGGREGATION = 'aggregation'
    DATA_LOADING = 'loading'
    JOIN = 'join'
    UNION = 'union'
    TYPE1 = 'type1'
    TYPE2 = 'type2'


class DataStorageTypes(Enum) :
    LOCAL_STORAGE = 'localstorage'
    CLOUD_STORAGE = 'cloudstorage'
    CLOUD_SQL = 'cloudsql'
    BIG_QUERY = 'bigquery'


class WriteMethod(Enum) :
    OVERWRITE = 'overwrite'
    APPEND = 'append'
    MERGE = 'merge'


class TargetHandlingMethod(Enum):
    CREATE='create'
    NONE='none'

class PCollectionDelimiter(Enum):
    DELIMITER=','

class JoinTypes(Enum):
    FULL='full',
    RIGHT='right',
    LEFT='left'
    INNER='inner'