from abc import ABCMeta

import IIGEnums


class IIGPipeline:
    def __init__(self, pipeline_id, pipeline_name, pipeline_description, subject_area, domain):
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.pipeline_description = pipeline_description
        self.subject_area = subject_area
        self.domain = domain
        self.transform_stages = []
        self._pipeline_runner_options = None

    @property
    def pipeline_runner_options(self):
        return self._pipeline_runner_options

    @pipeline_runner_options.setter
    def pipeline_runner_options(self, pipeline_runner_options):
        if isinstance(pipeline_runner_options, IIGPipelineRunOptions):
            self._pipeline_runner_options = pipeline_runner_options
        else:
            raise ValueError("Invalid type. Input is not of type IIGPipelineRunOptions")

    def addTransforms(self, stage_transform):
        if isinstance(stage_transform, IIGBaseTransform):
            self.transform_stages.append(stage_transform)
        else:
            raise ValueError("Inalid type. Input is not of type IIGTransformStage")

    def getTransformByID(self, stage_id):
        for stage in self.transform_stages:
            if stage.stage_id == stage_id :
                return stage
        return None

    def getCountofTransforms(self):
        return len(self.transform_stages)

    def getTransformsGenerator(self):
        self.transform_stages.sort(key=lambda transform:transform.stage_seq_no)
        for transform in self.transform_stages:
            yield transform


class IIGPipelineRunOptions:
    def __init__(self, job_name, project, runner, staging_location, temp_location, **custom_options):
        self.job_name = job_name
        self.project = project
        self.runner = runner
        self.staging_location = staging_location
        self.temp_location = temp_location
        self.custom_options = custom_options


class IIGConnectionProperties(object):
    __metaclass__ = ABCMeta

    def __init__(self, connection_id, connection_name):
        self.connection_id = connection_id
        self.connection_name = connection_name
        self.conn_dict = {}


class IIGLocalFileConnProperties(IIGConnectionProperties):
    def __init__(self, connection_id, connection_name):
        super(IIGLocalFileConnProperties, self).__init__(connection_id, connection_name)
        self.base_path=None

    def getConnectionString(self):
        return self.base_path


class IIGCloudStorageConnProperties(IIGConnectionProperties):
    def __init__(self, connection_id, connection_name):
        super(IIGCloudStorageConnProperties, self).__init__(connection_id, connection_name)
        self.storage_bucket = None
        self.base_path = None
        self.conn_dict = None

    def getConnectionString(self):
        return self.storage_bucket + self.base_path


class IIGCloudSQLConnParameters(IIGConnectionProperties):
    def __init__(self, connection_id, connection_name):
        super(IIGCloudSQLConnParameters, self).__init__(connection_id, connection_name)
        self.conn_dict = None

    def dbDetails(self, db_host, db_port, db_name, db_username, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_username = db_username
        self.db_password = db_password
        self.db_name = db_name

    def getConnectionString(self):
        return {"host": self.db_host, "port": self.db_port, "user": self.db_username, "passwd": self.db_password,
                "db": self.db_name}


class IIGTransformFactory:
    @classmethod
    def createTransformStage(cls, stage_id, stage_seq_no, stage_type, stage_name, stage_description):
        if stage_type == IIGEnums.TransformStageTypes.FILE_READER.value:
            return IIGFileReaderStage(stage_id, stage_seq_no, stage_type, stage_name, stage_description)
        elif stage_type == IIGEnums.TransformStageTypes.RDBMS_READER.value:
            return IIGRDBMSReaderStage(stage_id, stage_seq_no, stage_type, stage_name, stage_description)
        if stage_type == IIGEnums.TransformStageTypes.FILE_WRITER.value:
            return IIGFileWriterStage(stage_id, stage_seq_no, stage_type, stage_name, stage_description)
        elif stage_type == IIGEnums.TransformStageTypes.RDBMS_WRITER.value:
            return IIGRDBMSWriterStage(stage_id, stage_seq_no, stage_type, stage_name, stage_description)
        elif stage_type == IIGEnums.TransformStageTypes.DATA_VALIDATION.value or \
                        stage_type == IIGEnums.TransformStageTypes.DATA_STANDARDIZATION.value:
            return IIGSISODataProcessing(stage_id, stage_seq_no, stage_type, stage_name, stage_description)
        elif stage_type == IIGEnums.TransformStageTypes.JOIN.value:
            return IIGMISOJoinProcessing(stage_id, stage_seq_no, stage_type, stage_name, stage_description)


__metaclass__ = type


class IIGBaseTransform(object) :
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name, stage_description):
        self.stage_id = stage_id
        self.stage_seq_no = stage_seq_no
        self.stage_type = stage_type
        self.stage_name = stage_name
        self.stage_description = stage_description
        self.predecessor_stage_id = predecessor_stage_id
        self.successor_stage_id = successor_stage_id

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGBaseSISOTransform(IIGBaseTransform):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name, stage_description):
        super(IIGBaseSISOTransform, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                   stage_type, stage_name, stage_description)
        self._input_schema = None
        self._output_schema = None
        self._error_schema = None

    @property
    def input_schema(self):
        return self._input_schema

    @input_schema.setter
    def input_schema(self, input_schema):
        if isinstance(input_schema, IIGDataSchema):
            self._input_schema = input_schema
        else:
            raise ValueError("Invalid Argument. input_schema is not of type IIGDataSchema")

    @property
    def output_schema(self):
        return self._output_schema

    @output_schema.setter
    def output_schema(self, output_schema):
        if isinstance(output_schema, IIGDataSchema):
            self._output_schema = output_schema
        else:
            raise ValueError("Invalid Argument. output_schema is not of type IIGDataSchema")

    @property
    def error_schema(self):
        return self._error_schema

    @error_schema.setter
    def error_schema(self, error_schema):
        if isinstance(error_schema, IIGDataSchema):
            self._error_schema = error_schema
        else:
            raise ValueError("Invalid Argument. error_schema is not of type IIGDataSchema")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()

class IIGIOStage(IIGBaseSISOTransform):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGIOStage, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type,
                                         stage_name, stage_description)
        self._connection_properties = None

    @property
    def connection_properties(self):
        return self._connection_properties

    @connection_properties.setter
    def connection_properties(self, connection_properties):
        if isinstance(connection_properties, IIGConnectionProperties):
            self._connection_properties = connection_properties
        else:
            raise ValueError("Invalid Argument. Connection Properties is not of type ConnectionProperties")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGFileReaderStage(IIGIOStage):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGFileReaderStage, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                 stage_type, stage_name, stage_description)
        self.column_delimiter = ','
        self._connection_properties = None

    def fileProperties(self, folder_path, file_prefix, file_name, file_compression, column_delimiter):
        self.folder_path = folder_path
        self.file_prefix = file_prefix
        self.file_name = file_name
        self.file_compression = file_compression
        self.column_delimiter = column_delimiter

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGRDBMSReaderStage(IIGIOStage):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGRDBMSReaderStage, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                  stage_type, stage_name, stage_description)
        self._source_sql = None
        self._connection_properties = None

    @property
    def source_sql(self):
        return self._source_sql

    @source_sql.setter
    def source_sql(self, source_sql):
        self._source_sql = source_sql

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGFileWriterStage(IIGIOStage):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGFileWriterStage, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                 stage_type, stage_name, stage_description)
        self.write_mode = IIGEnums.WriteMethod.OVERWRITE.value
        self.target_mode = IIGEnums.TargetHandlingMethod.CREATE.value
        self._connection_properties = None

    def fileProperties(self, folder_path, file_prefix, file_name, file_compression, column_delimiter, write_mode, target_mode):
        self.folder_path = folder_path
        self.file_prefix = file_prefix
        self.file_name = file_name
        self.file_compression = file_compression
        self.column_delimiter = column_delimiter
        self.write_mode = write_mode
        self.target_mode = target_mode

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGRDBMSWriterStage(IIGIOStage):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGRDBMSWriterStage, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                  stage_type, stage_name, stage_description)
        self.write_mode = IIGEnums.WriteMethod.OVERWRITE.value
        self.target_mode = IIGEnums.TargetHandlingMethod.NONE.value
        self._connection_properties = None

    def dbProperties(self, table_name, write_mode, target_mode):
        self.table_name = table_name
        self.write_mode = write_mode
        self.target_mode = target_mode

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGSISODataProcessing(IIGBaseSISOTransform):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGSISODataProcessing, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                    stage_type, stage_name, stage_description)
        self._src_to_tgt_mapping = None

    @property
    def src_to_tgt_mapping(self):
        return self._src_to_tgt_mapping

    @src_to_tgt_mapping.setter
    def src_to_tgt_mapping(self, src_to_tgt_mapping):
        if isinstance(src_to_tgt_mapping, IIGSrcToTgtMapping):
            self._src_to_tgt_mapping = src_to_tgt_mapping
        else:
            raise ValueError("Invalid type. Input not of type IIGSrcToTgtMapping ")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGSISOAggregation(IIGSISODataProcessing):
    def _init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                stage_description):
        super(IIGSISOAggregation, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                 stage_type, stage_name, stage_description)
        self._src_to_tgt_mapping = None
        self.group_keys = []

    def addGroupingColumn(self, key_column):
        if isinstance(key_column, IIGDataColumn):
            self.group_keys.append(key_column)
        else:
            raise ValueError("Invalid type. Input not of type IIGDataColumn")

    def getGroupingColumns(self):
        return self.group_keys

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGBaseMISOTransform(IIGBaseTransform):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGBaseMISOTransform, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                   stage_type, stage_name, stage_description)
        self.input_schema = []
        self._output_schema = None
        self._error_schema = None

    def getInputSchemaGenerator(self):
        for in_schema in self.input_schema :
            yield in_schema

    def getInputSchemaByName(self, source_name):
        for in_schema in self.input_schema:
            if in_schema.source_name == source_name :
                return in_schema
        return None

    def addInputSchema(self, input_schema):
        if isinstance(input_schema, IIGDataSchema):
            self.input_schema.append(input_schema)
        else :
            ValueError("Invalid Value. Input should be of type IIGDataSchema ")

    @property
    def output_schema(self):
        return self._output_schema

    @output_schema.setter
    def output_schema(self, output_schema):
        if isinstance(output_schema, IIGDataSchema):
            self._output_schema = output_schema
        else:
            raise ValueError("Invalid Argument. output_schema is not of type IIGDataSchema")

    @property
    def error_schema(self):
        return self._error_schema

    @error_schema.setter
    def error_schema(self, error_schema):
        if isinstance(error_schema, IIGDataSchema):
            self._error_schema = error_schema
        else:
            raise ValueError("Invalid Argument. error_schema is not of type IIGDataSchema")

    @property
    def src_to_tgt_mapping(self):
        return self._src_to_tgt_mapping

    @src_to_tgt_mapping.setter
    def src_to_tgt_mapping(self, src_to_tgt_mapping):
        if isinstance(src_to_tgt_mapping, IIGSrcToTgtMapping):
            self._src_to_tgt_mapping = src_to_tgt_mapping
        else:
            raise ValueError("Invalid type. Input not of type IIGSrcToTgtMapping ")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGMISOJoinProcessing(IIGBaseMISOTransform):
    def __init__(self, stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id, stage_type, stage_name,
                 stage_description):
        super(IIGMISOJoinProcessing, self).__init__(stage_id, stage_seq_no, predecessor_stage_id, successor_stage_id,
                                                    stage_type, stage_name, stage_description)
        self._join_condition = None

    @property
    def join_condition(self):
        return self._join_condition

    @join_condition.setter
    def join_condition(self, join_condition):
        if isinstance(join_condition, IIGJoinCondition):
            self._join_condition = join_condition
        else:
            raise ValueError("Invalid type. Input not of type JoinCondition ")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGSrcToTgtMapping:

    def __init__(self):
        self.src_to_tgt_col_mappings = []

    def getSrcToTgtColMappingGenerator(self):
        for src_to_tgt_col_mapping in self.src_to_tgt_col_mappings:
            yield src_to_tgt_col_mapping

    def addSrcToTgtColMapping(self, src_to_tgt_col_mapping):
        if isinstance(src_to_tgt_col_mapping, IIGSrcToTgtColumnMapping):
            self.src_to_tgt_col_mappings.append(src_to_tgt_col_mapping)
        else:
            raise ValueError("Invalid Argument. Input must be of type IIGSrcToTgtColumnMapping")

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGSrcToTgtColumnMapping:

    def __init__(self):
        self.src_columns = []
        self.mapping_functions = []
        self._tgt_column = None

    def addSrcColumn(self, src_column):
        if isinstance(src_column, IIGDataColumn):
            self.src_columns.append(src_column)
        else:
            raise ValueError('Invalid type.Must be of type IIGDataColumn ')

    def getSrcColumnGenerator(self):
        for src_column in self.src_columns:
            yield src_column

    @property
    def tgt_column(self):
        return self._tgt_column

    @tgt_column.setter
    def tgt_column(self, tgt_column):
        if isinstance(tgt_column, IIGDataColumn):
            self._tgt_column = tgt_column
        else :
            raise ValueError("Invalid input type. src_column must be of type IIGDataColumn ")

    def addMappingFunction(self, mapping_function):
        if isinstance(mapping_function, IIGDataFunction) :
            self.mapping_functions.append(mapping_function)
        else:
            raise ValueError("Invalid type.Must be of type IIGDataFunction ")

    def getMappingFunctionGenerator(self):
        for mapping_function in self.mapping_functions:
            yield mapping_function

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGJoinCondition:
    def __init__(self):
        self.left_schema = ''
        self.right_schema = ''
        self.left_columns = []
        self.right_columns = []
        self.join_type = ''

    def addLeftJoinColumns(self, left_column):
        if isinstance(left_column, IIGDataColumn) :
            self.left_columns.append(left_column)

    def getLeftJoinColumns(self):
        return self.left_columns

    def getLeftJoinColumnByName(self, source_name, column_name):
        for left_column in self.left_columns :
            if left_column.source_name == source_name and left_column.column_name == column_name:
                return left_column
        return None

    def addRightJoinColumns(self, right_column):
        if isinstance(right_column, IIGDataColumn):
            self.left_columns.append(right_column)

    def getRightJoinColumns(self):
        return self.right_columns

    def getRightJoinColumnByName(self, source_name, column_name):
        for right_column in self.right_columns :
            if right_column.source_name == source_name and right_column.column_name == column_name:
                return right_column
        return None

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGDataFunction:

    def __init__(self, function_name, function_parameters, function_action):
        self.function_name = function_name
        self.function_parameters = function_parameters
        self.function_action = function_action

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGDataSchema:

    def __init__(self, source_name):
        self.source_name = source_name
        self.columns = []

    def addColumn(self, col):
        if isinstance(col, IIGDataColumn):
            self.columns.append(col)
        else:
            raise ValueError('Invalid Type. Input must be of type IIGDataColumn')

    def getSchemaAsDict(self):
        return {column.column_name: column.data_type for column in self.columns}

    def getColumnNamesAsList(self):
        return [column.column_name for column in self.columns]

    def getDataTypesAsList(self):
        return [column.data_type for column in self.columns]

    def getColumnByName(self, source_name, column_name):
        for column in self.columns :
            if column.source_name == source_name and column.column_name == column_name:
                return column

    def getDataColumnGenerator(self):
        for column in self.columns:
            yield column

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()


class IIGDataColumn:
    def __init__(self, source_name, column_name, data_type, nullable=False):
        self.source_name = source_name
        self.column_name = column_name
        self.data_type = data_type
        self.nullable = nullable

    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return self.__str__()
