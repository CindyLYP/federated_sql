select t1.address, sum(t2.amount) from account as t1 join record as t2 on t1.u_id = t2.user_id join t3 on t2.id = t3.id where t1.age > 20 and t2.year='2021' group by t1.address
