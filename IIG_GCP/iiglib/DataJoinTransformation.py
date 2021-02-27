from collections import OrderedDict
import apache_beam as beam
import IIGEnums


class DataJoinTransformation(beam.PTransform):

    def __init__(self, stage_metadata):
        super(DataJoinTransformation, self).__init__()
        self.stage_metadata = stage_metadata

        self.left_source_name = self.stage_metadata.join_condition.left_schema
        self.leftkeys_dict = OrderedDict(
            (left.column_name, None) for left in self.stage_metadata.join_condition.left_columns)
        self.leftvalues_dict = OrderedDict((col_name, None) for col_name in self.stage_metadata.getInputSchemaByName(
            self.left_source_name).getColumnNamesAsList() if col_name not in self.leftkeys_dict)

        self.right_source_name = self.stage_metadata.join_condition.right_schema
        self.rightkeys_dict = OrderedDict(
            (right.column_name, None) for right in self.stage_metadata.join_condition.right_columns)
        self.rightvalues_dict = OrderedDict((col_name, None) for col_name in self.stage_metadata.getInputSchemaByName(
            self.right_source_name).getColumnNamesAsList() if col_name not in self.rightkeys_dict)

        self.output_dict = OrderedDict((out, None) for out in self.stage_metadata.output_schema.getColumnNamesAsList())

    def _extract_input_pvalues(self, pvalueish):
        try:
            return pvalueish, tuple(pvalueish.viewvalues())
        except AttributeError:
            pcolls = tuple(pvalueish)
            return pcolls, pcolls

    def keyvalueGenerator(self, element, pcoll_tag):
        key_dict = self.leftkeys_dict if pcoll_tag == self.left_source_name else self.rightkeys_dict
        value_dict = self.leftvalues_dict if pcoll_tag == self.left_source_name else self.rightvalues_dict

        for key in key_dict.keys():
            key_dict[key] = element[key]
        for key in value_dict.keys():
            value_dict[key] = element[key]

        print "Generated Key ", key_dict
        print "Generated Values ", value_dict
        return (tuple(key_dict.values()), value_dict)

    class JoinTypeHandler(beam.DoFn):

        def process(self, element, outer):
            print "DataJoin JoinTypeHandler Input ", element
            key, join_result = element
            if outer.stage_metadata.join_condition.join_type == IIGEnums.JoinTypes.INNER.value:
                if len(join_result[outer.left_source_name]) > 0 and len(join_result[outer.right_source_name]) > 0:
                    print "DataJoin Inner Join"
                    yield element
            elif outer.stage_metadata.join_condition.join_type == IIGEnums.JoinTypes.LEFT.value:
                if len(join_result[outer.left_source_name]) > 0:
                    print "DataJoin Left Outer Join "
                    yield element
            elif outer.stage_metadata.join_condition.join_type == IIGEnums.JoinTypes.RIGHT.value:
                if len(join_result[outer.right_source_name]) > 0:
                    print "DataJoin Right Outer Join "
                    yield element
            elif outer.stage_metadata.join_condition.join_type == IIGEnums.JoinTypes.FULL.value:
                print "DataJoin Full Outer Join "
                yield element

    class DenormalizeResults(beam.DoFn):

        def process(self, element, outer):
            print "DataJoin - Formatting Results ", str(element)
            key, join_result = element

            if len(join_result[outer.left_source_name]) < len(join_result[outer.right_source_name]) :
                outer_result = join_result[outer.left_source_name]
                inner_result = join_result[outer.right_source_name]
            else :
                outer_result = join_result[outer.right_source_name]
                inner_result = join_result[outer.left_source_name]

            common_key_dict = OrderedDict()
            for i, k in enumerate(outer.leftkeys_dict.keys()):
                common_key_dict[k] = key[i]
            for i, k in enumerate(outer.rightkeys_dict.keys()):
                common_key_dict[k] = key[i]

            for orow in outer_result:
                for k in common_key_dict.keys():
                    try:
                        outer.output_dict[k] = common_key_dict[k]
                    except:
                        continue
                outer.output_dict.update(orow)
                if len(inner_result) == 0:
                    print "DataJoin formatted result ", outer.output_dict
                    yield outer.output_dict
                else:
                    for irow in inner_result:
                        outer.output_dict.update(irow)
                        print "DataJoin formatted result ", outer.output_dict
                        yield outer.output_dict

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
        join_type = joined_out | "Join Type Handler " >> beam.ParDo(self.JoinTypeHandler(), self)
        format_results = join_type | "Format Join Results  " >> beam.ParDo(self.DenormalizeResults(), self)
        return format_results