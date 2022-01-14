from server.parse import *


def receive(federated_sql: str, mode='fed'):

    graph = parse_federated_sql(federated_sql)
    res = graph.execute()

    return res


# federated_sql = "select t1.address, t1.age-t2.age " \
#                 "from 1001.account as t1 join 1002.record as t2 on t1.u_id = t2.user_id " \
#                 "where t1.age + 2 > 22 and t2._year='2021' " \
#                 "group by t1.address limit 1"

# receive(federated_sql)



