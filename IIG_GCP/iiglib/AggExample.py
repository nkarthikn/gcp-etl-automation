import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def key_value(self, keyvalue) :
    print "processing record ", keyvalue
    key = (str(keyvalue).split(',')[0])
    value = ','.join(str(keyvalue).split(',')[1:])
    print "Split values ", key, "Values ", value
    return (key, value)


def combineFnAdapter(self, element):
    print "processing element ", element
    key, value_list = element
    return (key, len(value_list))

if __name__ == '__main__' :

    print 'Employee File '
    p = beam.Pipeline(options=PipelineOptions())
    pcoll = p | 'ReadEmpFile' >> beam.io.textio.ReadFromText('D:\\testing\\emp.txt')
    pcoll = pcoll | "KeyValue" >> beam.Map(lambda x: ((str(x).split(',')[0], str(x).split(',')[1]), str(x).split(',')[2]))
    pcoll = pcoll | "CombinebyKey " >> beam.CombinePerKey(lambda x: (len(x), len(x), len(x)))
    pcoll = pcoll | "Flatmap " >> beam.Map(lambda x: list(x[0]) + list(x[1]))
    pcoll | beam.io.textio.WriteToText('D:\\testing\\empagg.txt')

    output = p.run()
    output.wait_until_finish()
