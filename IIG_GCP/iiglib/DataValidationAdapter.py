import DataValidationLibrary as dv
from enum import Enum

class ValidatorActions(Enum) :
    NULL_CHECK = 'nullcheck'
    DATATYPE_INT = 'datatypeint'
    DATATYPE_FLOAT = 'datatypefloat'
    DATATYPE_NUMERIC = 'datatypenumeric'
    RANGE_OF_VALUES = 'rangeofvalues'
    GREATER_THAN = 'greaterthan'

validatorFunctionMap = {
    ValidatorActions.NULL_CHECK: dv.nullCheck,
    ValidatorActions.DATATYPE_INT: dv.datatypeInt,
    ValidatorActions.DATATYPE_FLOAT: dv.datatypeFloat,
    ValidatorActions.DATATYPE_NUMERIC: dv.datatypeNumeric,
    ValidatorActions.RANGE_OF_VALUES: dv.rangeofvalues,
    ValidatorActions.GREATER_THAN: dv.greaterthan
}

def __get_action_enum(action_str) :
    try :
        action_enum = ValidatorActions(action_str)
        return action_enum
    except ValueError :
        pass
    return None

def get_function_for(action_str) :
    action_enum = __get_action_enum(action_str)
    if action_enum is None :
        return None
    return validatorFunctionMap.get(action_enum)