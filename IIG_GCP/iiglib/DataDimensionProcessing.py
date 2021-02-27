from collections import OrderedDict
import apache_beam as beam
from apache_beam.metrics import Metrics

class Type1ProcessingTransform(beam.PTransform):

    def __init__(self, stage_metadata):
        super(Type1ProcessingTransform, self).__init__()
        self.input_rec_count = Metrics.counter("Type1Processing", "inputrecords")
        self.output_rec_count = Metrics.counter("Type1Processing", "outputrecords")

        self.stage_metadata = stage_metadata
        self.left_source_name = self.stage_metadata.join_condition.left_schema
        self.leftkeys_dict = OrderedDict(
            (left.column_name, None) for left in self.stage_metadata.join_condition.left_columns)
        self.right_source_name = self.stage_metadata.join_condition.right_schema
        self.rightkeys_dict = OrderedDict(
            (right.column_name, None) for right in self.stage_metadata.join_condition.right_columns)
        self.joinvalue_dict = OrderedDict()
        for src_to_tgt_col in self.stage_metadata.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            found = False
            for group_col in self.leftkeys_dict.keys() + self.rightkeys_dict.keys():
                if src_to_tgt_col.tgt_column.column_name == group_col:
                    found = True
            if found == False:
                self.joinvalue_dict[src_to_tgt_col.tgt_column.column_name] = None
        self.output_dict = OrderedDict((out, None) for out in self.stage_metadata.output_schema.getColumnNamesAsList())

    def _extract_input_pvalues(self, pvalueish):
        try:
            return pvalueish, tuple(pvalueish.viewvalues())
        except AttributeError:
            pcolls = tuple(pvalueish)
            return pcolls, pcolls

    def keyvalueGenerator(self, element, pcoll_tag):
        self.input_rec_count.inc(1)
        print "Type1 Key Value Generator " + str(element)
        groupkey_dict = self.leftkeys_dict if pcoll_tag == self.left_source_name else self.rightkeys_dict
        for group_key in groupkey_dict.keys():
            groupkey_dict[group_key] = element[group_key]

        for joinvalue_key in self.joinvalue_dict.keys():
            self.joinvalue_dict[joinvalue_key] = element[joinvalue_key]

        print "Generated Key ", groupkey_dict
        print "Generated Values ", self.joinvalue_dict
        return (tuple(groupkey_dict.values()), self.joinvalue_dict)

    def type1Processing(self, element):
        print "Post Join condition ", element
        key, join_result = element
        if len(join_result[self.left_source_name]) == 0:
            print "Returning right ", (key, join_result[self.right_source_name][0])
            return (key, join_result[self.right_source_name][0])
        else:
            print "Returning left ", (key, join_result[self.left_source_name][0])
            return (key, join_result[self.left_source_name][0])

    def formatResults(self, element):
        print "Dimension Processing - Format Results ", element
        grp_key, grp_join_dict = element
        grp_key_dict = OrderedDict(zip(self.rightkeys_dict.keys(), list(grp_key)))

        grp_key_dict.update(grp_join_dict)
        for out_column in self.output_dict.keys():
            self.output_dict[out_column] = grp_key_dict[out_column]

        self.output_rec_count.inc(1)
        print "Formatted output ", self.output_dict
        return self.output_dict

    def expand(self, pcolls):
        # assuming pcolls[0] will contain the same dictionary sent from IIGPipeline
        # based on the _extract_input_pvalues method
        print "Inside expand method ", type(pcolls), pcolls, dir(pcolls)
        pcolls_dict = OrderedDict(pcolls)

        for pcoll_tag in pcolls_dict.keys():
            keyvalue_pcoll = pcolls_dict[pcoll_tag] | "KeyValueGenerator " + pcoll_tag >> beam.Map(
                self.keyvalueGenerator, pcoll_tag)
            pcolls_dict[pcoll_tag] = keyvalue_pcoll

        joined_out = pcolls_dict | "JoinSource&Target " >> beam.CoGroupByKey()
        type1_out = joined_out | "Type1 processing " >> beam.Map(self.type1Processing)
        format_results = type1_out | "Format Type1 Results  " >> beam.Map(self.formatResults)
        return format_results

class Type2ProcessingTransform(beam.PTransform):

    def __init__(self, stage_metadata):
        super(Type2ProcessingTransform, self).__init__()
        self.input_rec_counts = Metrics.counter('Type2Processing', 'inputrecords')
        self.output_rec_counts = Metrics.counter('Type2Processing', 'outputrecords')
        self.active_rec_counts = Metrics.counter('Type2Processing', 'activerecords')
        self.inactive_rec_counts = Metrics.counter('Type2Processing', 'inactiverecords')

        self.stage_metadata = stage_metadata
        self.left_source_name = self.stage_metadata.join_condition.left_schema
        self.leftkeys_dict = OrderedDict(
            (left.column_name, None) for left in self.stage_metadata.join_condition.left_columns)
        self.right_source_name = self.stage_metadata.join_condition.right_schema
        self.rightkeys_dict = OrderedDict(
            (right.column_name, None) for right in self.stage_metadata.join_condition.right_columns)
        self.joinvalue_dict = OrderedDict()
        self.type2_dict = OrderedDict()
        for src_to_tgt_col in self.stage_metadata.src_to_tgt_mapping.getSrcToTgtColMappingGenerator():
            found = False
            for group_col in self.leftkeys_dict.keys() + self.rightkeys_dict.keys():
                if src_to_tgt_col.tgt_column.column_name == group_col:
                    found = True
            if found == False:
                self.joinvalue_dict[src_to_tgt_col.tgt_column.column_name] = None
                for mapping in src_to_tgt_col.getMappingFunctionGenerator():
                    if mapping.function_parameters["type2Flag"] == 'Y':
                        self.type2_dict[src_to_tgt_col.tgt_column.column_name] = True
                    else:
                        self.type2_dict[src_to_tgt_col.tgt_column.column_name] = False

        self.output_dict = OrderedDict((out, None) for out in self.stage_metadata.output_schema.getColumnNamesAsList())

    def _extract_input_pvalues(self, pvalueish):
        try:
            return pvalueish, tuple(pvalueish.viewvalues())
        except AttributeError:
            pcolls = tuple(pvalueish)
            return pcolls, pcolls

    def keyvalueGenerator(self, element, pcoll_tag):
        self.input_rec_counts.inc(1)
        groupkey_dict = self.leftkeys_dict if pcoll_tag == self.left_source_name else self.rightkeys_dict
        for group_key in groupkey_dict.keys():
            groupkey_dict[group_key] = element[group_key]

        for joinvalue_key in self.joinvalue_dict.keys():
            self.joinvalue_dict[joinvalue_key] = element[joinvalue_key]

        print "Type 2 Generated Key ", groupkey_dict
        print "Type 2 Generated Values ", self.joinvalue_dict
        return (tuple(groupkey_dict.values()), self.joinvalue_dict)

    class Type2Handling(beam.DoFn) :

        def process(self, element, outer):
            print "Type 2 Processing element ",element
            key, join_result = element
            if len(join_result[outer.left_source_name]) == 0:
                print "Type 2 Returning right ", (key, join_result[outer.right_source_name][0])
                yield (key, join_result[outer.right_source_name][0])
            elif len(join_result[outer.right_source_name]) == 0:
                print "Type 2 Returning left ", (key, join_result[outer.left_source_name][0])
                yield (key, join_result[outer.left_source_name][0])
            elif len(join_result[outer.left_source_name]) > 0 and len(join_result[outer.right_source_name]) > 0:
                src_dict = join_result[outer.left_source_name][0]
                tgt_dict = join_result[outer.right_source_name][0]
                outvalue_dict = OrderedDict()

                type2_col_change = False
                for value_col in outer.joinvalue_dict.keys():
                    outvalue_dict[value_col] = tgt_dict[value_col] if src_dict[value_col] is None else src_dict[value_col]
                    if outer.type2_dict[value_col] and src_dict[value_col] != tgt_dict[value_col]:
                        type2_col_change = True

                outer.active_rec_counts.inc(1)
                yield (key, outvalue_dict)

                if type2_col_change :
                    print "Type 2 Inactive Record"
                    outer.inactive_rec_counts.inc(1)
                    yield beam.pvalue.TaggedOutput('history_record', (key, tgt_dict))


    def formatResults(self, element):
        grp_key, grp_join_dict = element
        print "Type 2 Processing - Format Results ", element, grp_key
        grp_key_dict = OrderedDict(zip(self.rightkeys_dict.keys(), list(grp_key)))

        self.output_dict.update(grp_key_dict)
        self.output_dict.update(grp_join_dict)
        self.output_rec_counts.inc(1)
        print "Type 2 Formatted Output ", self.output_dict
        return self.output_dict

    def expand(self, pcolls):
        # assuming pcolls[0] will contain the same dictionary sent from IIGPipeline
        # based on the _extract_input_pvalues method
        print "Type 2 Expand - PColls Received ", pcolls
        pcolls_dict = OrderedDict(pcolls)

        for pcoll_tag in pcolls_dict.keys():
            keyvalue_pcoll = pcolls_dict[pcoll_tag] | "KeyValueGenerator " + pcoll_tag >> beam.Map(
                self.keyvalueGenerator, pcoll_tag)
            pcolls_dict[pcoll_tag] = keyvalue_pcoll

        joined_out = pcolls_dict | "Join Source and Target " >> beam.CoGroupByKey()
        type2_out = joined_out | "Type2 processing " >> beam.ParDo(Type2ProcessingTransform.Type2Handling(),
                                                                   self).with_outputs('history_record',
                                                                                      main='active_record')
        union_out = [type2_out.active_record, type2_out.history_record] | "Union Active and History " >> beam.Flatten()
        format_results = union_out | "Format Type2 Results  " >> beam.Map(self.formatResults)
        return format_results


if __name__ == '__main__':
    test = {"hello":"world", "again":"test"}
    print Type1ProcessingTransform({"Dummy":""})._extract_input_pvalues(test)[0]
