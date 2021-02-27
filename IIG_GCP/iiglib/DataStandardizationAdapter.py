import DataStandardizationLibrary as ds
from enum import Enum


class StandardizationActions(Enum):
    INITCAP = 'initcap'
    UPPERCASE = 'uppercase'
    LOWERCASE = 'lowercase'
    SUBSTRING = 'substring'
    STRIP = 'strip'
    PREFIX = 'prefix'
    SUFFIX = 'suffix'
    REMOVE_SPLCHARS = 'removeSplChars'


standardizationFunctionMap = {
    StandardizationActions.INITCAP: ds.initcap,
    StandardizationActions.UPPERCASE: ds.uppercase,
    StandardizationActions.LOWERCASE: ds.lowercase,
    StandardizationActions.SUBSTRING: ds.substring,
    StandardizationActions.STRIP: ds.strip,
    StandardizationActions.PREFIX: ds.prefix,
    StandardizationActions.SUFFIX: ds.suffix,
    StandardizationActions.REMOVE_SPLCHARS: ds.removeSplChars
}


def __get_action_enum(action_str):
    try :
        action_enum = StandardizationActions(action_str)
        return action_enum
    except ValueError :
        pass
    return None


def get_function_for(action_str):
    action_enum = __get_action_enum(action_str)
    if action_enum is None :
        return None
    return standardizationFunctionMap.get(action_enum)