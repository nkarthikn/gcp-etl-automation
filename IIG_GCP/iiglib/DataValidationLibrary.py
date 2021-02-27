
def nullCheck(inputValue, parameterJSON):
    if str(inputValue).strip(' ') == '' or inputValue is None:
        return False
    else:
        return True


def datatypeInt(inputValue, parameterJSON):
    try:
        int(inputValue)
        return True
    except ValueError:
        return False


def datatypeFloat(inputValue, parameterJSON):
    try:
        float(inputValue)
        return True
    except ValueError:
        return False


def datatypeNumeric(inputValue, parameterJSON):
    return datatypeInt and datatypeFloat


def rangeofvalues(inputValue, parameterJSON):
    if not datatypeNumeric(inputValue, parameterJSON):
        return False

    if float(inputValue) in range(float(parameterJSON['minVal']),float(parameterJSON['maxVal'])):
        return True
    else:
        return False


def greaterthan(inputValue, parameterJSON):
    if not datatypeNumeric(inputValue, parameterJSON):
        return False
    elif not datatypeNumeric(parameterJSON['compareValue'], parameterJSON):
        raise "Invalid parameter exception "
    elif inputValue <= parameterJSON['compareValue']:
            return False
    return True




