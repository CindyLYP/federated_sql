from server.parse import *


def receive(federated_sql: str, mode='fed'):

    graph = parse_federated_sql(federated_sql)
    graph.execute()

    return None


# federated_sql = "select sum(distinct t1.id-t2.id) " \
#                 "from 1001.account t1 join 1002.record as t2 on t1.u_id = t2.user_id join 1003.t3 on t2.id = t3.id " \
#                 "where t1.age-t2.age > 20 and t2.year='2021' " \
#                 "group by t1.address, t2.id  order by desc t1.address limit 20;"
federated_sql = "select t1.address, sum(t2.amount) " \
                "from 1001.account as t1 join 1002.record as t2 on t1.u_id = t2.user_id " \
                "where t1.age > 20 and t2._year='2021' " \
                "group by t1.address"
receive(federated_sql)

