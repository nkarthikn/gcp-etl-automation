from collections import OrderedDict
import apache_beam as beam
import DataStandardizationAdapter as dva
from apache_beam.metrics import Metrics

class DataStandardizationTransform(beam.PTransform):
    def __init__(self, stage_metadata):
        super(DataStandardizationTransform, self).__init__()
        self.stage_metadata = stage_metadata

    def expand(self, pcoll):
        return pcoll | "Data Standardization " >> beam.ParDo(DataStandardizationFn(self.stage_metadata))


class DataStandardizationFn(beam.DoFn):
    def __init__(self, transform_stage):
        super(DataStandardizationFn, self).__init__()
        self.transform_stage = transform_stage
        self.input_rec_count = Metrics.counter('Standardization', 'inputrecords')
        self.output_rec_count = Metrics.counter('Standardization', 'inputrecords')

    def process(self, element):
        self.input_rec_count.inc(1)
        out_data_dict = OrderedDict((out, "") for out in self.transform_stage.output_schema.getColumnNamesAsList())

        print "Standardizing element ", element, out_data_dict
        for col_mapping in self.transform_stage.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            out_data_dict[col_mapping.tgt_column.column_name] = element[col_mapping.src_columns[0].column_name]
            for standardization_function in col_mapping.getMappingFunctionGenerator():
                result = dva.get_function_for(standardization_function.function_name)(
                    element[col_mapping.src_columns[0].column_name],
                    standardization_function.function_parameters)
                out_data_dict[col_mapping.tgt_column.column_name] = result

        self.output_rec_count.inc(1)
        print "Standardization Output is ", out_data_dict
        yield out_data_dict
