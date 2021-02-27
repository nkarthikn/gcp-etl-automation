from collections import OrderedDict
from apache_beam.transforms import core
from apache_beam.metrics import Metrics
import apache_beam as beam
import DataAggregationAdapter as dga


class DataAggregationTransform(beam.PTransform):

    def __init__(self, stage_metadata):
        super(DataAggregationTransform, self).__init__()
        self.input_rec_count = Metrics.counter('DataAggregation', 'inputrecords')
        self.output_rec_count = Metrics.counter('DataAggregation', 'outputrecords')

        self.stage_metadata = stage_metadata
        self.groupkey_dict = OrderedDict((grp_col.column_name, None) for grp_col in self.stage_metadata.group_keys)
        self.aggregate_dict = OrderedDict((src_to_tgt_col.tgt_column.column_name, None) for src_to_tgt_col in
                                          self.stage_metadata.src_to_tgt_mapping.getSrcToTgtColMappingGenerator() if
                                          src_to_tgt_col.tgt_column.column_name not in self.groupkey_dict)
        self.output_dict = OrderedDict((out, None) for out in self.stage_metadata.output_schema.getColumnNamesAsList())

    def keyvalueGenerator(self, element):
        self.input_rec_count.inc(1)
        print "Generating key value pair ", element

        for group_key in self.groupkey_dict.keys():
            self.groupkey_dict[group_key] = element[group_key]

        for src_to_tgt_col in self.stage_metadata.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            found = False
            for group_col in self.groupkey_dict.keys():
                if src_to_tgt_col.tgt_column.column_name == group_col:
                    found = True
            if found == False:
                self.aggregate_dict[src_to_tgt_col.tgt_column.column_name] = element[
                    src_to_tgt_col.src_columns[0].column_name]

        print "Aggregate Generated Key ", self.groupkey_dict
        print "Aggregate Generated Values ", self.aggregate_dict
        return (tuple(self.groupkey_dict.values()), self.aggregate_dict)

    def formatResults(self, element):
        print "Final Output from Aggregation ", element
        grp_key, grp_agg_dict = element
        grp_key_dict = OrderedDict(zip(self.groupkey_dict.keys(), list(grp_key)))
        print "Output in dict ", grp_key_dict, grp_agg_dict, self.output_dict
        grp_key_dict.update(grp_agg_dict)
        for out_column in self.output_dict.keys():
            self.output_dict[out_column] = grp_key_dict[out_column]

        self.output_rec_count.inc(1)
        print "Data Aggregation - Formatted output ", self.output_dict
        return self.output_dict

    def expand(self, pcoll):
        pcoll = pcoll | "KeyValueGenerator" >> beam.Map(self.keyvalueGenerator)
        pcoll = pcoll | "Aggregate by Key " >> beam.CombinePerKey(
            AggregateCombineFn(self.aggregate_dict.keys(), self.stage_metadata.src_to_tgt_mapping))
        pcoll = pcoll | "Format Results " >> beam.Map(self.formatResults)
        return pcoll

class AggregateCombineFn(core.CombineFn):

    def __init__(self, aggregate_header, src_to_tgt_mapping ):
        super(AggregateCombineFn, self).__init__()
        self.src_to_tgt_mapping = src_to_tgt_mapping
        self.aggregate_header = aggregate_header

    def create_accumulator(self, *args, **kwargs):
        print "Inside create accumulator "
        return ({'rowcount':0}, OrderedDict((header, None) for header in self.aggregate_header))

    def add_input(self, count_aggregates, element, *args, **kwargs):
        print "Inside add_input ", count_aggregates, "***", element
        row_count, inter_aggregates_dict = count_aggregates
        row_count['rowcount'] = row_count['rowcount'] + 1

        print "Check Values ", element, inter_aggregates_dict
        for col_mapping in self.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            for aggregation_function in col_mapping.getMappingFunctionGenerator() :
                print "Processing aggregation function ", aggregation_function,
                result = dga.get_function_for(aggregation_function.function_name)(
                    inter_aggregates_dict[col_mapping.tgt_column.column_name],
                    element[col_mapping.tgt_column.column_name])
                inter_aggregates_dict[col_mapping.tgt_column.column_name] = result

        print "Output from add_input ", (row_count, inter_aggregates_dict.values())
        return (row_count, inter_aggregates_dict)

    def merge_accumulators(self, count_aggregates, *args, **kwargs):
        print "Inside merge accumulator ", count_aggregates
        total_count_aggregates = None

        for count_aggregate in count_aggregates:
            if total_count_aggregates is None:
                print "Merging accumulators first time ", total_count_aggregates
                total_count_aggregates = count_aggregate
            else:
                print "Subsequent calls for merge accumulators ", total_count_aggregates, count_aggregate
                in_row_count, in_agg_values = count_aggregate
                total_row_count, total_agg_values = total_count_aggregates
                combined_aggregate = self.add_input(total_count_aggregates, in_agg_values)
                """ignoring the row count returned in this call. Calculating based on input values directly """
                combined_row_count = in_row_count['rowcount'] + total_row_count['rowcount']
                total_count_aggregates = (combined_row_count, combined_aggregate[1])

        print "merge accumulators output ", total_count_aggregates
        return total_count_aggregates

    def extract_output(self, count_aggregates, *args, **kwargs):
        print "Extract output ", count_aggregates
        row_count, aggregate_dict = count_aggregates

        for col_mapping in self.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            for aggregation_function in col_mapping.getMappingFunctionGenerator() :
                result = dga.get_output_function_for(aggregation_function.function_name)(
                    aggregate_dict[col_mapping.tgt_column.column_name], row_count['rowcount'])
                aggregate_dict[col_mapping.tgt_column.column_name] = result

        print "Output of combine function ", aggregate_dict
        return aggregate_dict