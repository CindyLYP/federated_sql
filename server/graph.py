import copy

from utils.operator import *
from utils.tools import *
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


def psi4join(id4clients):
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


def psi4where(clients: dict, unit: ConditionUnit):
    if unit.condition_type == "local":
        tb = unit.prefix_column.tb
        res = clients[tb].filter_by_condition(unit)
    else:
        prefix_tbn = unit.prefix_column.tb
        suffix_tbn = unit.suffix_column.tb if unit.suffix_column.tb != "_UNK" else prefix_tbn
        value_tbn = unit.value.tb if unit.value.tb != "_UNK" else prefix_tbn
        prefix_data = clients[prefix_tbn].get_safety_data_by_column(unit.prefix_column)
        if unit.cal_s is not None:
            suffix_data = clients[suffix_tbn].get_safety_data_by_column(unit.suffix_column)
            tmp = map(calculate_func[unit.cal_s], prefix_data, suffix_data)
            prefix_data = list(tmp)
        values = clients[value_tbn].get_safety_data_by_column(unit.value)
        res = list(map(compare_func[unit.jd_s], prefix_data, values))

    return res


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
                psi_ids = psi4join(id4clients)
                # 样本对齐
                for c in clients.keys():
                    clients[c].align_rows(align_column=clients[c].join_column, transform=psi_ids[c], is_join=True)
            if it == "_where":
                print()
                federated_ids = []
                conditions = node.conditions
                conjunctions = node.conjunctions
                federated_ids.append(psi4where(clients, conditions[0]))
                for i, conj in enumerate(conjunctions):
                    fed_id1 = psi4where(clients, conditions[i+1])
                    if conj == "AND":
                        fed_id2 = federated_ids.pop()
                        new_ids = list(map(conj_func[conj], fed_id1, fed_id2))
                        federated_ids.append(new_ids)
                    else:
                        federated_ids.append(fed_id1)
                conj = "OR"
                while len(federated_ids) > 1:
                    fed_id1 = federated_ids.pop()
                    fed_id2 = federated_ids.pop()
                    new_ids = list(map(conj_func[conj], fed_id1, fed_id2))
                    federated_ids.append(new_ids)
                idxs = [i for i, flag in enumerate(federated_ids[0]) if flag]
                for c in clients.keys():
                    clients[c].filter_by_fed_ids(idxs)
            if it == "_select":
                pass










