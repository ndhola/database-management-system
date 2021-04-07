from flask import Flask, request
import flask
import json
from passlib.hash import bcrypt
import time

app = Flask(__name__)
app.config["DEBUG"] = True


def getStateOfDatabase():
    file = open("db1.txt", "r")
    state = []
    for line in file:
        if line == "\n" or line == " ":
            return False
        tableName = line.split("-->")[0]
        if line.split("-->")[1] == "\n" or line.split("-->")[1] == "":
            totalRows = 0
        else:
            totalRows = len(line.split("-->")[1].split("|"))
        state.append([tableName, totalRows])
    return state


def rawToMeta(tableName):
    file = open("dbmetadata.txt", "r")
    for line in file:
        line = line.replace("\n", "")
        tableMeta = line.split("-->")[0]
        columns = line.split("-->")[1].split("|")
        existingTableName = tableMeta.split(",")[0]
        primaryKey = tableMeta.split(",")[1].split("->")[1]
        if existingTableName == tableName:
            data = {
                "primary_key": primaryKey,
                "columns": {}
            }
            for columnData in columns:
                columnName = columnData.split("->")[0]
                columnMeta = columnData.split("->")[1].split(",")
                data["columns"][columnName] = {
                    "type": columnMeta[0],
                    "size": columnMeta[1]
                }
            return data
    return False


def rawToData(tableName):
    file = open("db1.txt", "r+")
    for line in file:
        existingTableName = line.split("-->")[0]
        rows = line.split("-->")[1].split("|")
        rowList = []
        if existingTableName == tableName:
            if rows[0] == "\n" or rows[0] == '':
                return False
            for row in rows:
                rowValues = row.split(",")
                rowList.append(rowValues)
            return rowList
    return False


def dataToRaw(tableName, rowList):
    for rowIndex in range(len(rowList)):
        rowList[rowIndex] = ",".join(rowList[rowIndex])
    rowList = "|".join(rowList)

    file = open("db1.txt")
    newContent = ""
    for line in file:
        existingTableName = line.split("-->")[0]
        if existingTableName == tableName:
            newLine = tableName + "-->" + rowList.replace("\n", "")
            newContent += newLine + "\n"
        else:
            newContent += line.replace("\n", "") + "\n"
    print("newContent", newContent)
    file.close()
    file = open("db1.txt", "w+")
    file.write(newContent)

    file = open("dbmetadata.txt", "r+")
    for line in file:
        print("line", line)
        tableMeta = line.split("-->")[0]
        existingTableName = tableMeta.split(",")[0]
        print("comparision", existingTableName, tableName)
        if(existingTableName == tableName):
            return line.split("-->")[1]
    return False


@app.route('/')
def hello():
    return "<h1>DBMS SERVER FOR GROUP 2</h1>"


@app.route('/state')
def getState():
    data = getStateOfDatabase()
    if data:
        return flask.jsonify(data)
    else:
        return data


@app.route('/create', methods=['POST'])
def createTable():
    request_data = request.get_json()
    tableName = request_data["tableName"]
    primaryKey = request_data["primary_key"]
    columnMetas = "|".join(request_data["columnMetas"])
    query = request_data["query"]
    msg = ""
    isTableCreated = False

    metaData = rawToMeta(tableName)

    file = open("dbmetadata.txt", "a+")
    if metaData:
        msg = "ERROR -> Table is Already exist"
    else:
        file.write(tableName + ",PK->" + str(primaryKey) +
                   ",FK->null" + "-->" + columnMetas + "\n")
        file.close()

        file = open("db1.txt", "a")
        file.write(tableName + "-->")
        file.write("\n")
        file.close()

        file = open("dump.txt", "a+")
        file.write(query)
        file.write("\n")
        file.close()
        isTableCreated = True
        msg = "SUCCESS -> Table Created Succussfully"

    return flask.jsonify({
        "msg": msg,
        "isTableCreated": isTableCreated
    })


@ app.route('/insert', methods=['POST'])
def insertQuery():
    request_data = request.get_json()
    tableName = request_data["table_name"]
    columnList = request_data["columnValues"]

    meta = rawToMeta(tableName)
    availableColoumns = list(meta["columns"].keys())
    data = rawToData(tableName)
    primaryKeyIndex = int(meta["primary_key"])

    if len(columnList) != len(availableColoumns):
        return "ERROR -> Column count is not matching with table: " + tableName + " Expected Column Count: " + str(len(availableColoumns))

    if data:
        for row in data:
            if row[primaryKeyIndex] == columnList[primaryKeyIndex]:
                return "ERROR -> Primary key must be unique, duplicate value found: " + str(row[primaryKeyIndex])
        data.append(columnList)
    else:
        data = [columnList]

    dataToRaw(tableName, data)

    return "SUCCESS -> Record Inserted"


@app.route("/update", methods=['POST'])
def updateQuery():
    request_data = request.get_json()
    tableName = request_data["table_name"]
    columnList = request_data["column_list"]
    condition = request_data["condition"]

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)

    if metaData:
        availableColumns = list(metaData["columns"].keys())
        requestedColumns = list(columnList.keys())
        for columnName in requestedColumns:
            if columnName not in availableColumns:
                return "ERROR -> Invalid column name: " + columnName
        conditionColumn = condition.split("=")[0].strip(" ")
        conditionValue = condition.split("=")[1].replace("'", "").strip(" ")

        if conditionColumn == None and conditionValue == None:
            return "ERROR -> Invalid Condition: " + condition

        if conditionColumn not in availableColumns:
            return "ERROR -> Invalid condition column name: " + conditionColumn
        else:
            conditionIndex = availableColumns.index(conditionColumn)
            isUpdated = False
            for row in data:
                if row[conditionIndex] == conditionValue:
                    isUpdated = True
                    for columnName in columnList:
                        index = availableColumns.index(columnName)
                        row[index] = columnList[columnName]

            dataToRaw(tableName, data)

            if isUpdated:
                return "SUCCESS -> 1 rows is updated in Table: " + tableName
            else:
                return "ERROR -> No Record found with " + conditionColumn + " is " + conditionValue
    else:
        return "ERROR -> Table not found with name: " + tableName


@ app.route('/select', methods=['POST'])
def selectQuery():
    request_data = request.get_json()

    tableName = request_data['table_name']
    columnList = request_data['column_names']
    condition = request_data["condition"]

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)

    if metaData:
        columnIndexes = []
        availabeColumns = list(metaData["columns"].keys())
        if columnList[0] != "*":
            for column in columnList:
                if column not in availabeColumns:
                    return "Invalid Column Name: " + column
            for columnIndex in range(len(availabeColumns)):
                if availabeColumns[columnIndex] in columnList:
                    columnIndexes.append(columnIndex)
        else:
            columnList = []
            for index in range(len(availabeColumns)):
                columnIndexes.append(index)
                columnList.append(availabeColumns[index])

        if condition:
            conditionColumn = condition.split("=")[0].strip(" ")
            conditionValue = condition.split(
                "=")[1].replace("'", "").strip(" ")

            availabeColumns = list(metaData["columns"].keys())

            if conditionColumn not in availabeColumns:
                return "Invalid Condition Column: " + conditionColumn

            for columnIndex in range(len(availabeColumns)):
                if conditionColumn == availabeColumns[columnIndex]:
                    conditionIndex = columnIndex

        values = []
        for row in data:
            data = []
            if condition:
                if row[conditionIndex] == conditionValue:
                    for index in columnIndexes:
                        data.append(row[index])
                    values.append(data)
            else:
                for index in columnIndexes:
                    data.append(row[index])
                values.append(data)

        data = {
            "columnNames": columnList,
            "columnValues": values,
            "isFetched": False if len(values) == 0 else True,
            "msg": "Total " + str(len(values)) + " row(s) is/are fetched."
        }
    else:
        return "Table does not exist"

    return flask.jsonify(data)


@app.route('/delete', methods=['POST'])
def deleteQuery():
    request_data = request.get_json()
    tableName = request_data["tableName"]
    conditionColumn = request_data["columnName"].strip(" ")
    conditionValue = request_data["columnValue"].replace("'", "").strip(" ")

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)
    if metaData:
        availableColumns = list(metaData["columns"].keys())

        if conditionColumn not in availableColumns:
            return "ERROR -> Invalid condition column name: " + conditionColumn
        else:
            conditionIndex = availableColumns.index(conditionColumn)

        data = list(
            filter(lambda row: row[conditionIndex] != conditionValue, data))
        print(len(data))
        dataToRaw(tableName, data)
    else:
        return "ERROR -> Table not found with name: " + tableName

    return "SUCCESS -> Record is deleted where column name: " + conditionColumn + " with value: " + conditionValue


@app.route("/dump", methods=['GET'])
def getDump():
    file = open("dump.txt")
    data = []
    for line in file:
        data.append(line)
    return flask.jsonify(data)


@app.route('/validate', methods=['POST'])
def isUserValid():
    isValid = False
    request_data = request.get_json()
    authentication = open('authentication.json')
    users = json.load(authentication)["users"]

    username = request_data["username"]
    password = request_data["password"]

    for user in users:
        if(username == user["username"] and bcrypt.verify(password, user["password"])):
            isValid = True

    validity = {
        "isValid": isValid
    }

    return flask.jsonify(validity)


app.run(debug=True, threaded=True)
