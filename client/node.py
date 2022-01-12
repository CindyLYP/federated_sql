from utils.tools import *


class Client:
    def __init__(self, client_id, table_name, columns, join_column) -> None:
        self.table_name = table_name
        self.columns = columns
        self.join_column = join_column
        self.id2col, self.col2id = {}, {}
        self.db, self.data = None, None
        self.fed_column = "fed_id"

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
            fed_idx = max(self.id2col.keys()) + 1
            self.id2col[fed_idx] = self.fed_column
            self.col2id[self.fed_column] = fed_idx
            i = 0
            for j, row in enumerate(self.data):
                self.data[j] = list(row) + [i]
                i += 1
        print()


