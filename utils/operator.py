

class Column:
    def __init__(self, name, table_name, other_name=None, _type="var", value=None) -> None:
        self.name = name
        self.tb = table_name
        self.other_name = other_name
        self.column_type = _type
        self.value = value


class Table:
    def __init__(self, name, other_name, join_column=None, client_id=None) -> None:
        self.name = name
        self.other_name = other_name
        self.join_column = join_column
        self.load_columns = []
        self.client_id = client_id


class BaseUnit:
    def __init__(self, has_columns=None):
        self.has_columns = has_columns


class SelectUnit(BaseUnit):
    def __init__(self, columns, has_columns) -> None:
        super(SelectUnit, self).__init__(has_columns=has_columns)
        self.columns = columns


class ColumnUnit:
    def __init__(self, prefix_column=None, suffix_column=None, calculation_symbol=None,
                 aggregation=None, is_group=False, _type="local", unit_name=None) -> None:
        self.prefix_column = prefix_column
        self.suffix_column = suffix_column
        self.cal_s = calculation_symbol
        self.agg = aggregation
        self.is_distinct = False
        self.is_group = is_group
        self.component_type = _type
        self.name = unit_name


class FromUnit(BaseUnit):
    def __init__(self, table_names: list, tables: dict, table_name_map: dict, has_columns=None) -> None:
        super(FromUnit, self).__init__(has_columns=has_columns)
        self.table_names = table_names
        self.tables = tables
        self.table_name_map = table_name_map
        self.columns = []


class WhereUnit(BaseUnit):
    def __init__(self, conditions=[], conjunctions=[], has_columns=[]) -> None:
        super(WhereUnit, self).__init__(has_columns=has_columns)
        self.conditions = conditions
        self.conjunctions = conjunctions


class ConditionUnit:
    def __init__(self, prefix_column=None, suffix_column=None, judgment_symbol=None, calculation_symbol=None,
                 value=None, _type="local") -> None:
        self.prefix_column = prefix_column
        self.suffix_column = suffix_column
        self.jd_s = judgment_symbol
        self.cal_s = calculation_symbol
        self.value = value
        self.component_type = _type


class GroupByUnit(BaseUnit):
    def __init__(self, columns=None, has_columns=None, _type="local") -> None:
        super(GroupByUnit, self).__init__(has_columns=has_columns)
        self.columns = columns
        self.component_type = _type


class HavingUnit:
    def __init__(self) -> None:
        pass


class LimitUnit(BaseUnit):
    def __init__(self, number) -> None:
        super(LimitUnit, self).__init__()
        self.number = number


class OrderUnit(BaseUnit):
    def __init__(self, columns=None, has_columns=None, mode='asc') -> None:
        super(OrderUnit, self).__init__(has_columns=has_columns)
        self.columns = columns
        self.mode = mode




