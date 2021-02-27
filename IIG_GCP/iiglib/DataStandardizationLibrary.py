
def initcap(inputValue, parameterJSON):
    return str(inputValue).title()

def uppercase(inputValue, parameterJSON):
    return str(inputValue).upper()

def lowercase(inputValue, parameterJSON) :
    return str(inputValue).upper()

def substring(inputValue, parameterJSON) :
    data_value = str(inputValue)
    startpos = parameterJSON['startpos']
    length = parameterJSON['length']
    if startpos is None or startpos <= 0 :
        startpos = 0
    if length is None or length <= 0 :
        length = len(data_value)
    return data_value[startpos:length]

def strip(inputValue, parameterJSON) :
    try :
        strip_char = parameterJSON['stripchar']
    except :
        strip_char = ' '
    return str(inputValue).strip(strip_char)

def prefix(inputValue, parameterJSON):
    try :
        prefix_text = parameterJSON['prefix']
        return prefix_text + str(inputValue)
    except :
        return str(inputValue)


def suffix(inputValue, parameterJSON):
    try :
        suffix_text = parameterJSON['suffix']
        return suffix_text + str(inputValue)
    except :
        return str(inputValue)

def removeSplChars(inputValue, parameterJSON) :
    try :
        spl_chars = parameterJSON['splchars']
        data_value = str(inputValue)
        for spl_char in spl_chars :
            data_value.replace(spl_char, '')
        return data_value
    except :
        return inputValue

