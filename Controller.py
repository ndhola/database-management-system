import re
import requests
import json
import time

INVALID_QUERY = "invalid query"

REMOTE_URL = "http://127.0.0.1:5000"

supportedQuery = ["SELECT", "UPDATE", "CREATE", "DELETE", "INSERT"]


def identifyQuery(query):
    if query.split()[0] in supportedQuery:
        return query.split()[0]
    else:
        return INVALID_QUERY


def updateQuery(query):
    matchGroups = re.match(
        "UPDATE\s([\w]+)\sSET\s([\w\s=,'\"]+)+\s?(WHERE)\s?([\w\s=,'\"]+)", query)
    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
        return "Condition is missing"

    tablesName = matchGroups.group(1)
    columns = matchGroups.group(2)
    columnList = {}
    for column in columns.split(","):
        columnName = column.split("=")[0].strip(" ")
        columnValue = column.split("=")[1].strip(" ")
        columnList[columnName] = columnValue

    print("Column List", columnList)

    if matchGroups.group(3) == "WHERE":
        condition = matchGroups.group(4)

    data = {
        "column_list": columnList,
        "table_name": tablesName,
        "condition": condition
    }
    print("data", data)
    return "In Progress"


def selectQuery(query):
    matchGroups = re.match(
        "SELECT\s([\w\s,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?(\w+[=><]\w+)?", query)

    print(matchGroups.groups())

    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
        return "Condition is missing"

    columnNames = matchGroups.group(1).split(",")
    for columnIndex in range(len(columnNames)):
        columnNames[columnIndex] = columnNames[columnIndex].strip(" ")
    tablesName = matchGroups.group(2)
    condition = False
    if matchGroups.group(3) == "WHERE":
        condition = matchGroups.group(4)

    data = {
        "column_names": columnNames,
        "table_name": tablesName,
        "condition": condition
    }
    response = requests.post(REMOTE_URL + "/select", json=data)
    return response.text


def insertQuery(query):
    matchGroups = re.match(
        "INSERT INTO ([A-Za-z0-9_]+)\sVALUES\s\(([a-z,A-Z0-9\s']+)\)", query)
    print(matchGroups.group(1))
    table_name = matchGroups.group(1)
    columnValues = matchGroups.group(2)

    print(columnValues)
    column_values = []
    for column in columnValues.split(","):
        column_values.append(column.strip(" "))
    print(column_values)
    insertdata = {
        "table_name": table_name,
        "columnValues": column_values
    }
    response = requests.post(REMOTE_URL + "/insert", json=insertdata)
    return response.text


def createQuery(query):
    matchGroups = re.match(
        "CREATE\sTABLE\s([\w]+)\s\(([a-zA-Z0-9_\s,]+)\)?", query)
    print(matchGroups.group(1))

    table_name = matchGroups.group(1)
    print(type(table_name))

    createData = {
        "tableName": table_name
    }

    print(matchGroups.group(2))
    columnMetas = []
    columnList = matchGroups.group(2).split(",")
    for columnIndex in range(len(columnList)):
        metadata = columnList[columnIndex].strip(" ").split(" ")
        columnName = metadata[0]
        columnType = metadata[1]
        columnLength = metadata[2]
        columnMetas.append(columnName + "->" + columnType + "," + columnLength)
        if(len(metadata) > 3):
            if(metadata[3] == "PK"):
                createData["primary_key"] = columnIndex

    createData["columnMetas"] = columnMetas

    response = requests.post(REMOTE_URL + "/create", json=createData)
    return response.text


def runParser(queryType, query):
    # "SELECT\s([\w,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?(\w+=*)?"
    switcher = {
        "SELECT": lambda: selectQuery(query),
        "UPDATE": lambda: updateQuery(query),
        "CREATE": lambda: createQuery(query),
        "INSERT": lambda: insertQuery(query)
    }
    return switcher.get(queryType, INVALID_QUERY)


def printLog(query, msg, executionTime):
    print("========LOG=========")
    print("QUERY: " + query + " \nEXECUTION TIME: " + str(executionTime) + " ns")
    print("Query Status: " + msg)
    print("====================")


db = {}

username = input("username: ")
password = input("password: ")

data = {
    "username": username,
    "password": password
}

response = requests.post(REMOTE_URL + "/validate", json=data)

isValid = json.loads(response.text)["isValid"]

if isValid:
    query1 = "CREATE TABLE customer2 (customer_name string 25 PK, customer_address string 25)"
    query = "SELECT * FROM customer1"
    query2 = "UPDATE customer SET customer_name= helly,customer_address= Surat WHERE customer_name=group2"
    query2 = "INSERT INTO customer1 VALUES (Jemis6, 140 Gautam Park)"
    queryType = identifyQuery(query)
    if(queryType != INVALID_QUERY):
        startTime = time.time()
        processQuery = runParser(queryType, query)
        msg = processQuery()
        executionTime = time.time() - startTime
        printLog(query, msg, executionTime)
    else:
        print("Invalid Query Type")
else:
    print("Invalid User")
