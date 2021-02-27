import apache_beam as beam

import IIGEnums


class DataIngestionTransform(beam.PTransform):
    def __init__(self, stage_metadata):
        super(DataIngestionTransform, self).__init__()
        self.stage_metadata = stage_metadata

    def formatResults(self, element):
        data_row_list = str(element).split(IIGEnums.PCollectionDelimiter.DELIMITER.value)
        in_data_dict = dict(zip(self.stage_metadata.output_schema.getColumnNamesAsList(), data_row_list))
        return in_data_dict

    def expand(self, pcoll):
        print "Ingestion Expand ", self.stage_metadata.stage_id, type(DataIngestionFactory.getIngestionClass(self.stage_metadata))
        pcoll = pcoll | self.stage_metadata.stage_name >> DataIngestionFactory.getIngestionClass(self.stage_metadata)
        pcoll = pcoll | "Format Ingested Records " >> beam.Map(self.formatResults)
        return pcoll


class DataIngestionFactory:
    @classmethod
    def getIngestionClass(cls, stage_metadata):
        if stage_metadata.stage_type == IIGEnums.TransformStageTypes.FILE_READER.value:
            if stage_metadata.file_prefix is None or stage_metadata.file_prefix == '':
                full_file_path = stage_metadata.connection_properties.base_path + stage_metadata.folder_path + stage_metadata.file_name
                print "Reading from ", full_file_path
                return beam.io.textio.ReadFromText(full_file_path)
            else:
                full_file_path = stage_metadata.connection_properties.base_path + stage_metadata.folder_path + stage_metadata.file_prefix + "*"
                print "Reading from ", full_file_path
                return beam.io.textio.ReadFromText(full_file_path)
        return None
