import copy

from utils.operator import *
from client.node import Client
from collections import Counter


def load_client(unit: FromUnit):
    local_clients = {

    }
    for tbn in unit.table_names:
        tb = unit.tables[tbn]
        client = Client(client_id=tb.client_id, table_name=tbn, columns=tb.load_columns, join_column=tb.join_column)
        client.load_data()
        local_clients[tbn] = client
    return local_clients


def psi(id4clients):
    id4psi = copy.deepcopy(id4clients)
    public_ids = []
    for j in id4clients:
        if not public_ids:
            public_ids = id4clients[j]
        else:
            public_ids = list(set(public_ids) & set(id4clients[j]))

    public_ids = {
        i: 1 for i in public_ids
    }
    for k in id4clients:
        c = Counter(id4clients[k])
        for i in public_ids.keys():
            public_ids[i] *= c[i]
        id4clients[k] = c
    for k in id4psi.keys():
        for i, e in enumerate(id4psi[k]):
            if e in public_ids.keys():
                id4psi[k][i] = (e, public_ids[e]//id4clients[k][e])
            else:
                id4psi[k][i] = (e, 0)
    return id4psi


class Graph:
    def __init__(self) -> None:
        self.node_dict = {

        }
        self.cmd = []

    def register(self, name, node):
        self.cmd.append(name)
        self.node_dict[name] = node

    def execute(self):
        clients = None
        for it in self.cmd:
            node = self.node_dict[it]
            if it == "_from":
                # 客户端本地数据导入内存
                clients = load_client(node)
                # 生成客户单将要对齐id
                id4clients = {
                    c_id: clients[c_id].generate_join_id() for c_id in clients.keys()
                }
                # 隐私求交集
                psi_ids = psi(id4clients)
                # 样本对齐
                for c in clients.keys():
                    clients[c].align_rows(align_column=clients[c].join_column, transform=psi_ids[c], is_join=True)
            if it == "_where":
                print()








