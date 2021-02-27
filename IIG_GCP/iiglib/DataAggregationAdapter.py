import DataAggregationLibrary as dg
from enum import Enum


class AggregationActions(Enum):
    SUM = 'sum'
    COUNT = 'count'
    MAX = 'max'
    MIN = 'min'
    RANK = 'rank'
    AVERAGE = 'avg'


aggregationFunctionMap = {
    AggregationActions.SUM: dg.sum,
    AggregationActions.COUNT: dg.count,
    AggregationActions.MAX: dg.max,
    AggregationActions.MIN: dg.min,
    AggregationActions.AVERAGE: dg.sum
}

aggregationFunctionOutputMap = {
    AggregationActions.SUM: dg.passthrough,
    AggregationActions.COUNT: dg.passthrough,
    AggregationActions.MAX: dg.passthrough,
    AggregationActions.MIN: dg.passthrough,
    AggregationActions.AVERAGE: dg.avg_output
}

def __get_action_enum(action_str):
    try :
        action_enum = AggregationActions(action_str)
        return action_enum
    except ValueError :
        pass
    return None


def get_function_for(action_str):
    action_enum = __get_action_enum(action_str)
    if action_enum is None :
        return None
    return aggregationFunctionMap.get(action_enum)


def get_output_function_for(action_str):
    action_enum = __get_action_enum(action_str)
    if action_enum is None :
        return None
    return aggregationFunctionOutputMap.get(action_enum)