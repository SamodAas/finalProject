from flask import Flask, render_template, request
import pandas as pd
import snowflake.connector as sf
from flask_pymongo import PyMongo
from datetime import datetime


NAVIGATION = ({'caption': "SQL queries", 'href': '/'}, {'caption': "Visualization", 'href': '/visualization'})

app = Flask(__name__, static_url_path= '/static')

# MongoDB configuration

app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/mydb"

mongo = PyMongo(app)
website_data = mongo.db.website_data
graph_comments = mongo.db.graph_comments

# Snowflake connector

user = "samodazzz"
password = "nd;]G7=z}jFkDD9"
account = "wh55479.us-east-2.aws"
database = "COVID19DB"
warehouse = "COMPUTE_WH"
schema = "PUBLIC"
role = "ACCOUNTADMIN"

conn = sf.connect(user = user, password = password, account = account)


# two types of queries, one used only on the back end for warehouse, dabatase and similar config,
# and the other one used for processing queries passed through GET

def run_general_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except Exception as e:
        print(e)
    cursor.close()

def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetch_pandas_all()
    cursor.close()
    return results


# Visualization of snowflake graph with possibility to comment

@app.route("/visualization", methods = ['POST', 'GET'])
def test():
    if request.method == 'POST':
        comment = request.form['comment']
        graph_comments.insert_one({"graph_id":1, "comment": comment, "postTime": datetime.now()})
    dbase = mongo.db.graph_comments
    return render_template("visualization.html", navigation = NAVIGATION, dbase = dbase.find())

# This route accepts SQL queries, sends them to snowflake and then to html file through FLASK.
# If there's an error it gets printed out on the website

@app.route("/")
def index():
    query = request.args.get("query")
    
    # Checking if there is query to send to snowflake APIE
    if not query:
        return(render_template("navigation_template.html", navigation = NAVIGATION))
    else:
        try:
            data_from_table = run_query(conn, query)
            sql_query = website_data.insert_one({"query": query, "timeDate": datetime.now()})
            return render_template("navigation_template.html", navigation = NAVIGATION, exception = '', tables = [data_from_table.to_html(classes = 'data')],titles = data_from_table.columns.values)    
        except Exception as e:
            return render_template("navigation_template.html", navigation = NAVIGATION, query_result = '', exception = e)




if __name__ == "__main__":
    run_general_query(conn,'use warehouse '+warehouse)
    run_general_query(conn, 'use database '+database)
    run_general_query(conn, 'use role '+role)
    app.run(port=5000, debug=True)