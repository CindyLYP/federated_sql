from flask import Flask, request
from server.node import receive

app = Flask(__name__)


@app.route("/federated_sql", methods=['GET', 'POST'])
def federated_sql():

    if request.method == 'POST':
        sql = request.form['sql']
        sql_result = receive(sql)

        res = {
            'msg': 'success!',
            'data': sql_result
        }
    else:
        res = {
            'msg': 'http method error, please use POST'
        }
    return res

