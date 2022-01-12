from utils.tools import dbop
from sqlast.App import SqlAst
import sqlparse
from sqlparse.tokens import _TokenType, Whitespace
from sqlparse.sql import IdentifierList, Identifier

fed_token = _TokenType()
white_space = fed_token.white_space

sql = "select t1.address, sum(t2.amount) " \
      "from account t1 join record as t2 on t1.u_id = t2.user_id join t3 on t2.id = t3.id " \
      "where t1.age > 20 and t2.year='2021' " \
      "group by t1.address, t2.id  order by t1.address"

tree = sqlparse.parse(sql)

# from t1 join t2 on eq1 join t3 on eq2
# from Identifier join Identifier on Comparison join Identifier on Comparison ..
tokens = [t for t in tree[0].tokens if not t.is_whitespace]
from_tokens = []
start_flag = False
fed_tables = []
for idx in range(len(tokens)):
      token = tokens[idx]
      if token.is_keyword and token.normalized == 'JOIN':
          start_flag = True
          assert type(tokens[idx+1]) == Identifier, "You have an error in your SQL syntax near [from] part"



