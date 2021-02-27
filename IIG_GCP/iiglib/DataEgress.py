import apache_beam as beam
import IIGEnums


class DataEgressTransform(beam.PTransform):
    def __init__(self, stage_metadata):
        super(DataEgressTransform, self).__init__()
        self.stage_metadata = stage_metadata

    def formatResults(self, element):
        return IIGEnums.PCollectionDelimiter.DELIMITER.value.join(map(str, tuple(element.values())))

    def expand(self, pcoll):
        pcoll = pcoll | "Format output records " >> beam.Map(self.formatResults)
        pcoll = pcoll | self.stage_metadata.stage_name >> DataEgressFactory.getEgressClass(self.stage_metadata)
        return pcoll


class DataEgressFactory:
    @classmethod
    def getEgressClass(cls, stage_metadata):
        if stage_metadata.stage_type == IIGEnums.TransformStageTypes.FILE_WRITER.value:
            if stage_metadata.file_prefix is None or stage_metadata.file_prefix == '':
                full_file_path = stage_metadata.connection_properties.base_path + stage_metadata.folder_path + stage_metadata.file_name
                return beam.io.textio.WriteToText(full_file_path)
            else:
                full_file_path = stage_metadata.connection_properties.base_path + stage_metadata.folder_path + stage_metadata.file_prefix
                return beam.io.textio.WriteToText(full_file_path)
        return None