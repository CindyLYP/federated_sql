import pandas as pd

from utils.tools import dbop
from utils.operator import *
import copy
import pandas

db = dbop
sql = "select account.address, account.u_id, account.age from account;"

columns = ["account.address", "account.u_id", "account.age"]

r_data = db.select_all(sql)
print(r_data)
df = pd.DataFrame(r_data, columns=columns)
print(df)
