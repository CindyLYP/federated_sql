from utils.operator import FromUnit
from utils.operator import *
from server.graph import Graph
import re
import copy
import sqlparse
from sqlparse.tokens import _TokenType, Whitespace, Operator
from sqlparse.sql import IdentifierList, Identifier, Comparison, Where, Operation, Token, Function

fed_token = _TokenType()
white_space = fed_token.white_space
float_token = fed_token.Literal.Number.Float
int_token = fed_token.Literal.Number.Integer

name_map = {}


def get_column(frag):
    if type(frag) == Identifier:
        ts = frag.tokens
        col_name, tb_name = ts[-1].value, ts[0].value
        column = Column(name=col_name, table_name=name_map[tb_name] if tb_name in name_map.keys() else tb_name)
    else:
        value = frag.value
        if "Float" in str(frag.ttype):
            value = float(value)
        elif "Integer" in str(frag.ttype):
            value = int(value)
        elif "String" in str(frag.ttype):
            value = value.replace("\'", "").replace("\"", "")
        column = Column(name="_UNK", table_name="_UNK", _type="const", value=value)
    return column


def parse_from_section(tokens):

    def generate_table(fragment, client_id):
        table_name, other_name = fragment.tokens[0].value, fragment.tokens[-1].value
        return Table(name=table_name, other_name=other_name, client_id=client_id)

    # from t1 join t2 on eq1 join t3 on eq2
    # from Identifier join Identifier on Comparison join Identifier on Comparison ..
    start_flag = False
    fed_tables = []

    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        if token.is_keyword and token.normalized == 'FROM':
            start_flag = True
            assert type(tokens[idx+3]) == Identifier, "You have an error in your SQL syntax near [from] part"
            fed_tables.append(generate_table(tokens[idx+3], tokens[idx+1].value))
            idx += 4
        if start_flag:
            if idx + 6 <= len(tokens) and \
                    tokens[idx].normalized == 'JOIN' and \
                    type(tokens[idx+3]) == Identifier and \
                    tokens[idx+4].normalized == 'ON' and \
                    type(tokens[idx+5]) == Comparison:
                tb = generate_table(tokens[idx+3], tokens[idx+1].value)

                comp = tokens[idx+5]
                left, right = comp.left.tokens[-1], comp.right.tokens[-1]
                fed_tables[-1].join_column = left.value
                tb.join_column = right.value
                fed_tables.append(tb)
                idx += 5
            else:
                break
        idx += 1

    tb_names = [tb.name for tb in fed_tables]
    tables = {
        tb.name: tb for tb in fed_tables
    }
    table_map = {
        tb.other_name: tb.name for tb in fed_tables
    }
    has_columns = [Column(name=tb.join_column, table_name=tb.name) for tb in fed_tables]
    unit = FromUnit(table_names=tb_names, tables=tables, table_name_map=table_map, has_columns=has_columns)
    return unit


def parse_where_section(where_token):
    # where condition and condition
    tokens = [t for t in where_token.tokens if not t.is_whitespace]

    conjunctions = [token.normalized for token in tokens[1:] if token.is_keyword]
    conditions = []
    columns = []

    for token in tokens:
        if type(token) == Comparison:
            sub_tokens = [t for t in token if not t.is_whitespace]
            assert len(sub_tokens) == 3, "only support condition like c1 - c2 = v"
            condition = ConditionUnit()
            if type(sub_tokens[0]) == Operation:
                op_tokens = [t for t in sub_tokens[0] if not t.is_whitespace]
                condition.prefix_column = get_column(op_tokens[0])
                condition.suffix_column = get_column(op_tokens[2])

                columns.extend([condition.prefix_column, condition.suffix_column])
                condition.cal_s = op_tokens[1].value

            elif type(sub_tokens[0]) == Identifier:
                condition.prefix_column = get_column(sub_tokens[0])
                columns.append(condition.prefix_column)

            condition.jd_s = sub_tokens[1].value
            condition.value = get_column(sub_tokens[2])
            tmp_cols = [col.tb for col in [condition.prefix_column, condition.suffix_column, condition.value]
                        if col is not None and col.column_type == "var"]
            if len(set(tmp_cols)) > 1:
                condition.condition_type = "multiply"
            else:
                condition.condition_type = "local"
            conditions.append(condition)

    res = WhereUnit(conditions=conditions, conjunctions=conjunctions, has_columns=columns)

    return res


def parse_select_section(select_token, is_dist):

    # select col, col op col, agg col, agg col op col
    # is distinct
    select_units = []
    columns = []
    is_distinct = is_dist
    for token in select_token.tokens:
        if token.is_keyword and token.normalized == "DISTINCT":
            is_distinct = True
        if type(token) == Token:
            continue
        s_token = token
        unit = ColumnUnit()
        unit.is_distinct = is_distinct
        if type(s_token) == Function:
            unit.agg = s_token.tokens[0].value
            tmp_tokens = s_token.tokens[1]
            for t in tmp_tokens.tokens:
                if t.is_keyword and t.normalized == "DISTINCT":
                    is_distinct = True
                if type(t) != Token:
                    s_token = t
                    break
        if type(s_token) == Operation:
            op_tokens = [t for t in s_token.tokens if not t.is_whitespace]
            unit.prefix_column = get_column(op_tokens[0])
            unit.suffix_column = get_column(op_tokens[2])
            columns.extend([unit.prefix_column, unit.suffix_column])
            unit.jd_s = op_tokens[1].value

        elif type(s_token) == Identifier:
            unit.prefix_column = get_column(s_token)
            columns.append(unit.prefix_column)
        unit.is_distinct = is_distinct
        is_distinct = False
        select_units.append(unit)

    res = SelectUnit(columns=select_units, has_columns=columns)

    return res


def parse_group_by_section(group_by_token):
    columns = []
    group_columns = []
    for token in group_by_token.tokens:
        if type(token) == Identifier:
            col = get_column(token)
            columns.append(col)
            group_columns.append(col)
    res = GroupByUnit(columns=group_columns, has_columns=columns)
    return res


def parse_order_by_section(order_by_token, p):
    columns = []
    order_columns = []
    for token in order_by_token.tokens:
        if type(token) == Identifier:
            col = get_column(token)
            columns.append(col)
            order_columns.append(col)
    res = OrderUnit(columns=order_columns, has_columns=columns, mode=p)
    return res


def parse_having_section(having_token):
    ...


def parse_federated_sql(raw_sql: str):
    '''
    select agg(column1 op column2), ..
    from tb1 join tb2 on tb1.id = tb2.id
    where column3 judgment_symbol value calculation_symbol column4 [and/or] ..
    group by column
    order by [desc, ase] column, ..

    '''

    graph = Graph()
    sql_keywords = []
    global name_map

    text = raw_sql.replace('\n', ' ').replace(';', '')
    tree = sqlparse.parse(text)
    tokens = [t for t in tree[0].tokens if not t.is_whitespace]

    for token in tokens:
        if token.is_keyword:
            sql_keywords.append(token.normalized)
        elif type(token) == Where:
            sql_keywords.append("WHERE")

    # from
    context_from = None
    if "FROM" in sql_keywords:
        context_from = parse_from_section(tokens)
        name_map = context_from.table_name_map

    # where
    context_where = None
    if "WHERE" in sql_keywords:
        where_token = None
        for token in tokens:
            if type(token) == Where:
                where_token = token
                break
        context_where = parse_where_section(where_token)

    # select
    assert "SELECT" in sql_keywords, "federated sql must include keyword select "
    select_tokens, is_distinct = [], False
    for idx, token in enumerate(tokens):
        if token.is_keyword and token.normalized == "SELECT":
            if tokens[idx+1].is_keyword and tokens[idx+1].normalized == 'DISTINCT':
                is_distinct = True
                j = idx+2
            else:
                j = idx+1
            select_tokens = tokens[j] if type(tokens[j]) == IdentifierList else \
                IdentifierList([tokens[j]])
            break

    context_select = parse_select_section(select_tokens, is_distinct)

    # group by
    context_group_by = None
    if "GROUP BY" in sql_keywords:
        group_by_token = None
        for idx, token in enumerate(tokens):
            if token.is_keyword and token.normalized == "GROUP BY":
                group_by_token = tokens[idx+1] if type(tokens[idx+1]) == IdentifierList else \
                                 IdentifierList([tokens[idx+1]])
                break

        context_group_by = parse_group_by_section(group_by_token)

    # order by
    context_order_by = None
    if "ORDER BY" in sql_keywords:
        order_by_token = None
        p = "asc"
        for idx, token in enumerate(tokens):
            if token.is_keyword and token.normalized == "ORDER BY":
                if tokens[idx+1].is_keyword:
                    p = tokens[idx+1].value
                    j = idx+2
                else:
                    j = idx+1

                order_by_token = tokens[j] if type(tokens[j]) == IdentifierList else \
                    IdentifierList([tokens[j]])
                break

        context_order_by = parse_order_by_section(order_by_token, p)

    context_limit = None
    if "LIMIT" in sql_keywords:
        for idx, token in enumerate(tokens):
            if token.is_keyword and token.normalized == "LIMIT":
                context_limit = LimitUnit(number=tokens[idx+1].value)

    contexts = [context_from, context_where, context_group_by, context_select, context_order_by, context_limit]

    for tbn in context_from.table_names:
        cols = []
        for context in contexts:
            if context is not None and context.has_columns is not None:
                for col in context.has_columns:
                    if col.column_type != "const" and col.tb == tbn:
                        cols.append(col.name)
        cols = list(set(cols))
        context_from.tables[tbn].load_columns = cols

    for i, name in enumerate(["_from", "_where", "_group_by", "_select", "_order_by", "_limit"]):
        if contexts[i] is not None:
            graph.register(name, contexts[i])

    return graph



