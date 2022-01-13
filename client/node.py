from utils.tools import *
from utils.operator import *


class Client:
    def __init__(self, client_id, table_name, columns, join_column) -> None:
        self.table_name = table_name
        self.columns = columns
        self.join_column = join_column
        self.id2col, self.col2id = {}, {}
        self.db, self.data = None, None
        self.fed_column = "fed_id"
        self.group_column = "fed_group"

        self.load_database(client_id)
        self.load_data()

    def load_database(self, client_id):
        self.db = dbop

    def load_data(self):
        load_sql = "select " + ",".join(self.columns) + f" from {self.table_name}" + f" order by {self.join_column}"
        self.data = self.db.select_all(load_sql)
        self.id2col = {
            idx: col for idx, col in enumerate(self.columns)
        }
        self.col2id = {
            col: idx for idx, col in enumerate(self.columns)
        }

    def generate_join_id(self):
        idx = self.col2id[self.join_column]
        join_ids = [row[idx] for row in self.data]
        return join_ids

    def align_rows(self, align_column: str, transform: list, is_join=False):
        idx = self.col2id[align_column]
        cur_data = []
        for i, [row, m] in enumerate(zip(self.data, transform)):
            assert row[idx] == m[0], "data mismatch"
            for _ in range(m[1]):
                cur_data.append(row)
        self.data = cur_data
        if is_join:
            # 生成联邦id
            fed_idx = max(self.id2col.keys()) + 1
            self.id2col[fed_idx] = self.fed_column
            self.col2id[self.fed_column] = fed_idx
            i = 0
            for j, row in enumerate(self.data):
                self.data[j] = list(row) + [i]
                i += 1
            # 生成联邦分组id
            fed_group_idx = max(self.id2col.keys()) + 1
            self.id2col[fed_group_idx] = self.group_column
            self.col2id[self.group_column] = fed_group_idx
            i = 0
            for j, row in enumerate(self.data):
                self.data[j] = list(row) + [i]

    def get_data_by_column(self, column):
        if column.column_type == "var":
            i = self.col2id[column.name]
            d = [row[i] for row in self.data]
        elif column.column_type == "const":
            d = [column.value for _ in self.data]
        else:
            raise RuntimeError(f"unknown type [{column.column_type}] for column {column.tb}.{column.name}")
        return d

    def filter_by_condition(self, unit: ConditionUnit):
        # where中的条件进行本地运算
        prefix_data = self.get_data_by_column(unit.prefix_column)
        if unit.cal_s is not None:
            suffix_data = self.get_data_by_column(unit.suffix_column)
            tmp = map(calculate_func[unit.cal_s], prefix_data, suffix_data)
            prefix_data = list(tmp)
        values = self.get_data_by_column(unit.value)
        res = list(map(compare_func[unit.jd_s], prefix_data, values))
        # fed_id = self.col2id[self.fed_column]
        #
        # res = [(self.data[i][fed_id], tmp[i]) for i in range(len(self.data))]
        return res

    def get_safety_data_by_column(self, column):

        # 暂用明文数据，实际考虑仅进行四则运算和比较操作可以采用同态加密等策略
        if column.column_type == "var":
            i = self.col2id[column.name]
            d = [row[i] for row in self.data]
        elif column.column_type == "const":
            d = [column.value for _ in self.data]
        else:
            raise RuntimeError(f"unknown type [{column.column_type}] for column {column.tb}.{column.name}")
        return d

    def filter_by_fed_ids(self, idxs):
        tmp = [self.data[i] for i in idxs]
        self.data = tmp

    def select_by_group(self):
        pass