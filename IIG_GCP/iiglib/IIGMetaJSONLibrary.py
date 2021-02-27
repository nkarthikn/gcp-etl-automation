import IIGEnums
import IIGMetamodel


class IIGMetaModelJSONParser:
    def loadIIGMetaModel(self, metadataJSON):
        self.metadataJSON = metadataJSON
        pipeline = self._createPipeline()
        pipeline.pipeline_runner_options = self._createPipelineRunOptions()
        pipeline.transform_stages = self._processTransforms(self.metadataJSON['transforms'])
        return pipeline

    def _createPipeline(self):
        print "Creating pipeline object "
        return IIGMetamodel.IIGPipeline(pipeline_id=self.metadataJSON['pipelineID'],
                                        pipeline_name=self.metadataJSON['pipelineName'],
                                        pipeline_description=self.metadataJSON['pipelineDescription'],
                                        subject_area=self.metadataJSON['subjectArea'],
                                        domain=self.metadataJSON['domain'])

    def _createPipelineRunOptions(self):
        print "Initializing Run Options"
        return IIGMetamodel.IIGPipelineRunOptions(job_name=self.metadataJSON['pipelineRunOptions']['jobName'],
                                                  project=self.metadataJSON['pipelineRunOptions']['project'],
                                                  runner=self.metadataJSON['pipelineRunOptions']['runner'],
                                                  secret=self.metadataJSON['pipelineRunOptions']['secret'],
                                                  staging_location=self.metadataJSON['pipelineRunOptions'][
                                                      'stagingLocation'],
                                                  temp_location=self.metadataJSON['pipelineRunOptions']['tempLocation'],
                                                  raw_location=self.metadataJSON['pipelineRunOptions']['rawLocation'],
                                                  semiprepared_location=self.metadataJSON['pipelineRunOptions'][
                                                      'semipreparedLocation'],
                                                  prepared_location=self.metadataJSON['pipelineRunOptions'][
                                                      'preparedLocation']
                                                  )

    def _processTransforms(self, transform_stages):
        transforms = []
        for transform_stage in transform_stages:

            if transform_stage['stageType'] == IIGEnums.TransformStageTypes.FILE_READER.value:
                transforms.append(self._processFileReader(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.FILE_WRITER.value:
                transforms.append(self._processFileWriter(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.RDBMS_READER.value:
                transforms.append(self._processRDBMSReader(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.RDBMS_READER.value:
                transforms.append(self._processRDBMSWriter(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.DATA_STANDARDIZATION.value:
                transforms.append(self._processDataStandardization(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.DATA_VALIDATION.value:
                transforms.append(self._processDataValidation(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.DATA_AGGREGATION.value:
                transforms.append(self._processDataAggregation(transform_stage))
            elif transform_stage['stageType'] == IIGEnums.TransformStageTypes.TYPE1.value or transform_stage[
                'stageType'] == IIGEnums.TransformStageTypes.TYPE2.value or transform_stage[
                'stageType'] == IIGEnums.TransformStageTypes.JOIN.value:
                transforms.append(self._processType1Processing(transform_stage))

        return transforms

    def _processFileReader(self, transform_stage):
        print "Loading File Reader Schema ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGFileReaderStage(stage_id=transform_stage['stageID'],
                                                        stage_seq_no=transform_stage['stageSeqNo'],
                                                        predecessor_stage_id=transform_stage['predecessorStageID'],
                                                        successor_stage_id=transform_stage['successorStageID'],
                                                        stage_type=transform_stage['stageType'],
                                                        stage_name=transform_stage['stageName'],
                                                        stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.fileProperties(folder_path=transform_stage['stageInput']['folderPath'],
                                     file_prefix=transform_stage['stageInput']['filePrefix'],
                                     file_name=transform_stage['stageInput']['fileName'],
                                     file_compression=transform_stage['stageInput']['fileCompression'],
                                     column_delimiter=transform_stage['stageInput']['fileDelimiter'])

        iig_transform.connection_properties = self._processFileConnProperties(
            transform_stage['stageInput']['connectionProperties'])

        return iig_transform

    def _processFileWriter(self, transform_stage):

        print "Inside File Writer ", transform_stage['stageOutput']
        iig_transform = IIGMetamodel.IIGFileWriterStage(stage_id=transform_stage['stageID'],
                                                        stage_seq_no=transform_stage['stageSeqNo'],
                                                        predecessor_stage_id=transform_stage['predecessorStageID'],
                                                        successor_stage_id=transform_stage['successorStageID'],
                                                        stage_type=transform_stage['stageType'],
                                                        stage_name=transform_stage['stageName'],
                                                        stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.fileProperties(folder_path=transform_stage['stageOutput']['folderPath'],
                                     file_prefix=transform_stage['stageOutput']['filePrefix'],
                                     file_name=transform_stage['stageOutput']['fileName'],
                                     file_compression=transform_stage['stageOutput']['fileCompression'],
                                     column_delimiter=transform_stage['stageOutput']['fileDelimiter'],
                                     write_mode=transform_stage['stageOutput']['writeMode'],
                                     target_mode=transform_stage['stageOutput']['targetMode']
                                     )
        iig_transform.connection_properties = self._processFileConnProperties(
            transform_stage['stageOutput']['connectionProperties'])

        return iig_transform

    def _processRDBMSReader(self, transform_stage):
        print "Inside RDBMS Reader ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGRDBMSReaderStage(stage_id=transform_stage['stageID'],
                                                         stage_seq_no=transform_stage['stageSeqNo'],
                                                         predecessor_stage_id=transform_stage['predecessorStageID'],
                                                         successor_stage_id=transform_stage['successorStageID'],
                                                         stage_type=transform_stage['stageType'],
                                                         stage_name=transform_stage['stageName'],
                                                         stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.source_sql = transform_stage['stageInput']['sourceSQL']
        iig_transform.connection_properties = self._processRDBMSConnProperties(
            transform_stage['stageInput']['connectionProperties'])

        return iig_transform

    def _processRDBMSWriter(self, transform_stage):
        print "Inside RDBMS Writer ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGRDBMSWriterStage(stage_id=transform_stage['stageID'],
                                                         stage_seq_no=transform_stage['stageSeqNo'],
                                                         predecessor_stage_id=transform_stage['predecessorStageID'],
                                                         successor_stage_id=transform_stage['successorStageID'],
                                                         stage_type=transform_stage['stageType'],
                                                         stage_name=transform_stage['stageName'],
                                                         stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.dbProperties(table_name=transform_stage['stageOutput']['tableName'],
                                   write_mode=transform_stage['stageOutput']['writeMode'],
                                   target_mode=transform_stage['stageOutput']['targetMode'])

        iig_transform.connection_properties = self._processRDBMSConnProperties(
            transform_stage['stageOutput']['connectionProperties'])

        return iig_transform

    def _processDataStandardization(self, transform_stage):
        print "Processing Data Standardization ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGSISODataProcessing(stage_id=transform_stage['stageID'],
                                                           stage_seq_no=transform_stage['stageSeqNo'],
                                                           predecessor_stage_id=transform_stage['predecessorStageID'],
                                                           successor_stage_id=transform_stage['successorStageID'],
                                                           stage_type=transform_stage['stageType'],
                                                           stage_name=transform_stage['stageName'],
                                                           stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.src_to_tgt_mapping = self._processMapping(transform_stage['mapping'],
                                                                iig_transform.input_schema,
                                                                iig_transform.output_schema)

        return iig_transform

    def _processDataValidation(self, transform_stage):
        print "Processing Data Validation ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGSISODataProcessing(stage_id=transform_stage['stageID'],
                                                           stage_seq_no=transform_stage['stageSeqNo'],
                                                           predecessor_stage_id=transform_stage['predecessorStageID'],
                                                           successor_stage_id=transform_stage['successorStageID'],
                                                           stage_type=transform_stage['stageType'],
                                                           stage_name=transform_stage['stageName'],
                                                           stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.src_to_tgt_mapping = self._processMapping(transform_stage['mapping'],
                                                                iig_transform.input_schema,
                                                                iig_transform.output_schema)

        return iig_transform

    def _processDataAggregation(self, transform_stage):
        print "Processing Data Aggregation ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGSISOAggregation(stage_id=transform_stage['stageID'],
                                                        stage_seq_no=transform_stage['stageSeqNo'],
                                                        predecessor_stage_id=transform_stage['predecessorStageID'],
                                                        successor_stage_id=transform_stage['successorStageID'],
                                                        stage_type=transform_stage['stageType'],
                                                        stage_name=transform_stage['stageName'],
                                                        stage_description=transform_stage['stageDescription'])

        iig_transform.input_schema = self._processSchema(transform_stage['stageInput']['name'],
                                                         transform_stage['stageInput']['schema'])
        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.src_to_tgt_mapping = self._processMapping(transform_stage['mapping'],
                                                                iig_transform.input_schema,
                                                                iig_transform.output_schema)

        iig_transform.group_keys = self._processGroupingKeys(transform_stage['groupingKeys'],
                                                             iig_transform.input_schema)

        return iig_transform

    def _processType1Processing(self, transform_stage):
        print "Processing Type1 ", transform_stage['stageName']
        iig_transform = IIGMetamodel.IIGMISOJoinProcessing(stage_id=transform_stage['stageID'],
                                                           stage_seq_no=transform_stage['stageSeqNo'],
                                                           predecessor_stage_id=transform_stage['predecessorStageID'],
                                                           successor_stage_id=transform_stage['successorStageID'],
                                                           stage_type=transform_stage['stageType'],
                                                           stage_name=transform_stage['stageName'],
                                                           stage_description=transform_stage['stageDescription'])

        for input_schema in transform_stage['stageInput']:
            iig_transform.addInputSchema(self._processSchema(input_schema['name'], input_schema['schema']))

        iig_transform.output_schema = self._processSchema(transform_stage['stageOutput']['name'],
                                                          transform_stage['stageOutput']['schema'])
        iig_transform.error_schema = self._processSchema(transform_stage['stageError']['name'],
                                                         transform_stage['stageError']['schema'])

        iig_transform.src_to_tgt_mapping = self._processMappingMISO(transform_stage['mapping'],
                                                                iig_transform.input_schema,
                                                                iig_transform.output_schema)

        iig_transform.join_condition = self._processJoinCondition(transform_stage['joinCondition'],
                                                                   iig_transform.input_schema)

        return iig_transform

    def _processSchema(self, source_name, data_schema):
        print "Processing Schema ", source_name, data_schema
        iig_schema = IIGMetamodel.IIGDataSchema(source_name)

        for column_name in data_schema.keys():
            data_type = data_schema[column_name]
            iig_column = IIGMetamodel.IIGDataColumn(source_name, column_name, data_type)
            iig_schema.addColumn(iig_column)

        return iig_schema

    def _processMapping(self, src_to_tgt_mapping, input_schema, output_schema):
        print "Processing Mapping ", src_to_tgt_mapping, input_schema, output_schema
        iig_src_to_tgt_mapping = IIGMetamodel.IIGSrcToTgtMapping()
        for mapping_column in src_to_tgt_mapping:
            iig_src_to_tgt_col = IIGMetamodel.IIGSrcToTgtColumnMapping()
            iig_src_to_tgt_col.addSrcColumn(
                input_schema.getColumnByName(mapping_column['inSchema'], mapping_column['inColumn']))
            iig_src_to_tgt_col.tgt_column = output_schema.getColumnByName(mapping_column['outSchema'],
                                                                          mapping_column['outColumn'])

            for mapping_function in mapping_column['functions']:
                iig_function = IIGMetamodel.IIGDataFunction(mapping_function['name'],
                                                            mapping_function['parameters'],
                                                            mapping_function['action'])
                iig_src_to_tgt_col.addMappingFunction(iig_function)

            iig_src_to_tgt_mapping.addSrcToTgtColMapping(iig_src_to_tgt_col)

        return iig_src_to_tgt_mapping

    def _processMappingMISO(self, src_to_tgt_mapping, input_schema, output_schema):
        print "Processing Mapping MISO ", src_to_tgt_mapping, "** ", input_schema, "** ", output_schema
        iig_src_to_tgt_mapping = IIGMetamodel.IIGSrcToTgtMapping()
        for mapping_column in src_to_tgt_mapping:
            iig_src_to_tgt_col = IIGMetamodel.IIGSrcToTgtColumnMapping()
            input_schema_for_mapping = [x for x in input_schema if x.source_name == mapping_column['inSchema']][0]
            iig_src_to_tgt_col.addSrcColumn(
                input_schema_for_mapping.getColumnByName(mapping_column['inSchema'], mapping_column['inColumn']))
            iig_src_to_tgt_col.tgt_column = output_schema.getColumnByName(mapping_column['outSchema'],
                                                                          mapping_column['outColumn'])

            for mapping_function in mapping_column['functions']:
                iig_function = IIGMetamodel.IIGDataFunction(mapping_function['name'],
                                                            mapping_function['parameters'],
                                                            mapping_function['action'])
                iig_src_to_tgt_col.addMappingFunction(iig_function)

            iig_src_to_tgt_mapping.addSrcToTgtColMapping(iig_src_to_tgt_col)

        return iig_src_to_tgt_mapping

    def _processFileConnProperties(self, conn_prop_schema):
        print "File Connection Properties ", conn_prop_schema
        connection_properties = IIGMetamodel.IIGCloudStorageConnProperties(
            connection_id=conn_prop_schema['connectionID'],
            connection_name=conn_prop_schema['connectionName'])
        if conn_prop_schema['fileStorageType'] == IIGEnums.DataStorageTypes.CLOUD_STORAGE.value:
            connection_properties.storage_bucket = conn_prop_schema['storageBucket']
            connection_properties.base_path = conn_prop_schema['basePath']
        elif conn_prop_schema['fileStorageType'] == IIGEnums.DataStorageTypes.LOCAL_STORAGE.value:
            connection_properties.base_path = conn_prop_schema['basePath']

        return connection_properties

    def _processRDBMSConnProperties(self, conn_prop_schema):
        print "RDBMS Connection Properties ", conn_prop_schema
        connection_properties = None

        if conn_prop_schema['databaseType'] == IIGEnums.DataStorageTypes.CLOUD_SQL.value:
            connection_properties = IIGMetamodel.IIGCloudSQLConnParameters(
                connection_id=conn_prop_schema['connectionID'],
                connection_name=conn_prop_schema['connectionName'])
            connection_properties.dbDetails(db_host=conn_prop_schema['databaseHost'],
                                            db_port=conn_prop_schema['databasePort'],
                                            db_name=conn_prop_schema['databaseName'],
                                            db_username=conn_prop_schema['databaseUserName'],
                                            db_password=conn_prop_schema['databasePassword'])

        return connection_properties

    def _processJoinCondition(self, join_mappings, input_schema):
        print "Processing Join Condition  ", join_mappings, " ******* ", input_schema
        jc = IIGMetamodel.IIGJoinCondition()
        jc.left_schema = join_mappings['left']['inputSchema']
        jc.right_schema = join_mappings['right']['inputSchema']
        jc.join_type = join_mappings['joinType']

        for left_col in join_mappings['left']['columns']:
            left_input_schema = [x for x in input_schema if x.source_name == join_mappings['left']['inputSchema']][0]
            jc.left_columns.append(left_input_schema.getColumnByName(join_mappings['left']['inputSchema'], left_col))

        for right_col in join_mappings['right']['columns']:
            right_input_schema = [x for x in input_schema if x.source_name == join_mappings['right']['inputSchema']][0]
            jc.right_columns.append(right_input_schema.getColumnByName(join_mappings['right']['inputSchema'], right_col))

        return jc

    def _processGroupingKeys(self, grouping_json, input_schema):
        print "Processing Aggregation ", grouping_json
        grouping_keys = []
        for cols in grouping_json :
            in_column = input_schema.getColumnByName(input_schema.source_name, cols)
            grouping_keys.append(in_column)

        return grouping_keys

if __name__ == '__main__':
    print("Hello World!")

    import json

    inputJSON = None
    with open("D:\Karthik\Architect\GoogleCloud\Schema\inputSchema_v8_agg.json") as meta:
        inputJSON = json.load(meta)
        print "dumping contents ", inputJSON

    myparser = IIGMetaModelJSONParser().loadIIGMetaModel(inputJSON)
    print "I am here ", myparser.pipeline_name, myparser.transform_stages
    for transform in myparser.getTransformsGenerator():
        print transform.stage_name, transform.stage_type
        print transform
