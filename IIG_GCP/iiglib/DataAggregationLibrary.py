def sum(current_value, input_value):
    print "Check Types ", type(current_value), type(input_value), input_value, current_value
    if current_value is None:
        current_value = 0
    input_value = float(input_value)
    return current_value + input_value

def count(current_value, input_value):
    if current_value is None:
        current_value = 0
    return current_value + 1

def max(current_value, input_value):
    print "Max Function ", current_value, input_value
    try :
        input_value = float(input_value)
        if current_value is None :
            current_value = float(0)
        return current_value if current_value > float(input_value) else float(input_value)
    except:
        if current_value is None:
            current_value = ''
        return current_value if current_value > input_value else input_value

def min(current_value, input_value):
    try :
        input_value = float(input_value)
        if current_value is None :
            current_value = float(0)
        return current_value if current_value < float(input_value) else float(input_value)
    except:
        if current_value is None:
            current_value = ''
        return current_value if current_value < input_value else input_value

def passthrough(current_value, count_value):
    return current_value

def avg_output(sum_value, count_value):
    return sum_value/count_value