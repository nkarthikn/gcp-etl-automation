from collections import OrderedDict
import apache_beam as beam
import DataValidationAdapter as dva
import IIGEnums
from apache_beam.metrics import Metrics

class DataValidationTransform(beam.PTransform):
    def __init__(self, stage_metadata):
        super(DataValidationTransform, self).__init__()
        self.stage_metadata = stage_metadata

    def expand(self, pcoll):
        pcoll_valid_schema = pcoll | "Schema Validation " >> beam.ParDo(SchemaValidationFn(self.stage_metadata))
        pcoll_valid_coldata = pcoll_valid_schema | "Column Validation " >> beam.ParDo(ColumnValidationFn(self.stage_metadata))
        return pcoll_valid_coldata


class SchemaValidationFn(beam.DoFn):
    def __init__(self, transform_stage):
        super(SchemaValidationFn, self).__init__()
        self.transform_stage = transform_stage

    def process(self, element):
        print "Schema Validation ", str(element)
        if len(element) == len(self.transform_stage.input_schema.getColumnNamesAsList()):
            print "Schema Validation successful ", str(element)
            yield element
        else:
            print "schema validation failed ", str(element)


class ColumnValidationFn(beam.DoFn):

    def __init__(self, transform_stage):
        super(ColumnValidationFn, self).__init__()
        self.transform_stage = transform_stage
        self.input_rec_count = Metrics.counter('ColumnValidation', 'inputrecords')
        self.valid_rec_count = Metrics.counter('ColumnValidation', 'validrecords')
        self.invalid_rec_count = Metrics.counter('ColumnValidation', 'invalidrecords')

    def process(self, element):
        self.input_rec_count.inc(1)
        out_data_dict = OrderedDict((out, "") for out in self.transform_stage.output_schema.getColumnNamesAsList())

        print "Column Validation ", element, out_data_dict
        rejectFlag = False
        errorMessage = ''
        for col_mapping in self.transform_stage.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            for validation_function in col_mapping.getMappingFunctionGenerator():
                result = dva.get_function_for(validation_function.function_name)(
                    element[col_mapping.src_columns[0].column_name],
                    validation_function.function_parameters)
                if result == False:
                    if validation_function.function_action == 'reject':
                        rejectFlag = True

                    errorMessage = errorMessage + 'Failed ' + validation_function.function_name + ' ' + \
                                   col_mapping.src_columns[0].column_name + ':' + element[
                                       col_mapping.src_columns[0].column_name]
                    print "Error Messsage ", errorMessage
            out_data_dict[col_mapping.tgt_column.column_name] = element[col_mapping.src_columns[0].column_name]

        if rejectFlag == False:
            self.valid_rec_count.inc(1)
            print "********** Returning Output ************** ", rejectFlag, out_data_dict
            yield out_data_dict
        else:
            self.invalid_rec_count.inc(1)