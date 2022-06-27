import json
import random
from datetime import datetime
import sqlite3

class gathi_param:
    def __init__(self, database, schema, table, objID, objName, PK, rules):
        self.database = database
        self.schema = schema
        self.table = table
        self.objID = objID
        self.objName = objName
        self.PK = PK
        self.rules = rules


def json_to_params(path = False):
    """
    :param path: the path of the json file
    :return: return the gathi_param object with all the attributes assigned
    """
    if path is False:
        path = input("Enter the path of your file: ")
    else:
        path = path

    with open(path, 'r') as f:
        data = json.load(f)

    param = gathi_param(data['Database'],data['Schema'],data['Table'],data['ObjId'],data['ObjName'],
                        data['PrimaryKey'], data['Rules'])
    return param

def params_to_query(parameters:gathi_param) -> str:
    """
    :param parameters: input is the gathi_param object
    :return: returns a list of queries depending on how many rules are there in the json file
    """

    query_list = []

    for i in range(len(parameters.rules)):
        db = parameters.database
        tbl = parameters.table
        cde = parameters.rules[i]['Arguments']['CDE']
        rulename = parameters.rules[i]['RuleName']
        date = datetime.now()
        ruledef = parameters.rules[i]['RuleDef']

        ruledef = ruledef.replace('$CDE', cde)

        if rulename == 'VALID_VALUES_LIST':
            ruledef = ruledef.replace('$val', parameters.rules[i]['Arguments']['val'])

        query = """
        insert into error_table
        (database, tableName, CDEName, REC_NBR, RuleName, Date)
        select '{}' as database, '{}' as tableName, '{}' as CDEName, REC_NBR, '{}' as RuleName, '{}' as Date
        from {}
        where {}
        """.format(db,tbl,cde,rulename,date,tbl, ruledef)

        query_list.append(query)

    return query_list


def run_job(query_list, db_path=False):
    """
    :param path: the path of the database file (because in the demo, we use sqlite,
                 if it is other database, depending on the login method, create the corresponding url)
    :return: return it will run the job and insert the error records into the target table
    """

    if db_path is False:
        db_path = input("Enter the path of your file: ")
    else:
        db_path = db_path

    conn = sqlite3.connect(db_path)

    for i in range(len(query_list)):
        current_query = query_list[i]
        print("executing the {} out of {} query".format(i+1, len(query_list)))
        print(test_query[i])
        cur = conn.cursor()
        cur.execute(current_query)
        conn.commit()

# Examples:
# test_param = json_to_params('D:\google\Gathi\sample_input.json')
#
# test_query = (params_to_query(test_param))
#
# run_job(test_query, db_path='D:\work\Database\DQtest.db')