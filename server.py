from flask import Flask, request
import flask
import json
from passlib.hash import bcrypt

app = Flask(__name__)
app.config["DEBUG"] = True


def getStateOfDatabase():
    file = open("db1.txt", "r")
    state = []
    for line in file:
        tableName = line.split("-->")[0]
        totalRows = len(line.split("-->")[1].split("|"))
        state.append("Table: " + tableName + " Total Rows: " + str(totalRows))
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


def isTableExist(tableName):
    file = open("dbmetadata.txt", "r+")
    for line in file:
        print("line", line)
        tableMeta = line.split("-->")[0]
        existingTableName = tableMeta.split(",")[0]
        print("comparision", existingTableName, tableName)
        if(existingTableName == tableName):
            return line.split("-->")[1]
    return False


def verifyColumn(columnName, metadata):
    data = metadata.split("|")
    for index in range(len(data)):
        existingColumn = data[index].split("->")[0]
        print("1", existingColumn, "2", columnName)
        if existingColumn == columnName:
            return index
    return -1


def parseConditionIndex(condition, metadata):
    columnName = condition.split("=")[0].strip(" ")
    index = verifyColumn(columnName, metadata)
    return index


def updateTable(rowList, tableName, file):
    print("In update table")
    for line in file:
        existingTableName = line.split("-->")[0]
        print("tableNames", existingTableName, tableName)
        if existingTableName == tableName:
            newLine = tableName + "-->" + rowList
            print("newLine", newLine)
            file.write(newLine + "\n")
        else:
            file.write(line + "\n")


@app.route('/')
def hello():
    return "Hello World updated 4!"


@app.route('/state')
def getState():
    data = getStateOfDatabase()
    return flask.jsonify(data)


@app.route('/create', methods=['POST'])
def createTable():
    request_data = request.get_json()
    tableName = request_data["tableName"]
    primaryKey = request_data["primary_key"]
    columnMetas = "|".join(request_data["columnMetas"])
    print(request_data)
    file = open("dbmetadata.txt", "a+")
    if(isTableExist(tableName)):
        return "Table is Already exist"
    else:
        file.write(tableName + ",PK->" + str(primaryKey) +
                   ",FK->null" + "-->" + columnMetas + "\n")
        file.close()
        file = open("db1.txt", "a")
        file.write(tableName + "-->")
        file.write("\n")
        file.close()

    return "Table Created Successfully"


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
                return "Invalid column name: " + columnName
        conditionColumn = condition.split("=")[0]
        conditionValue = condition.split("=")[1]

        if conditionColumn == None and conditionValue == None:
            return "Invalid Condition: " + condition

        if conditionColumn not in availableColumns:
            return "Invalid condition column name: " + conditionColumn
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
                return "1 rows is updated in Table: " + tableName
            else:
                return "No Record found with " + conditionColumn + " is " + conditionValue
    else:
        return "Table not found with name: " + tableName


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
            conditionColumn = condition.split("=")[0]
            conditionValue = condition.split("=")[1]

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
            "columnValues": values
        }
    else:
        return "Table does not exist"

    return flask.jsonify(data)


@ app.route('/insert', methods=['POST'])
def insertQuery():
    request_data = request.get_json()
    tableName = request_data["table_name"]
    columnList = request_data["columnValues"]

    meta = rawToMeta(tableName)
    availableColoumns = list(meta["columns"].keys())
    print("availableColumns", availableColoumns)
    data = rawToData(tableName)
    primaryKeyIndex = int(meta["primary_key"])

    if len(columnList) != len(availableColoumns):
        return "Column count is not matching with table: " + tableName

    print("data", data)

    if data:
        for row in data:
            print("existing data: ",
                  row[primaryKeyIndex], columnList[primaryKeyIndex])
            if row[primaryKeyIndex] == columnList[primaryKeyIndex]:
                return "Primary key must be unique"
        data.append(columnList)
    else:
        data = [columnList]

    dataToRaw(tableName, data)

    return "Record Inserted"


@app.route('/delete', methods=['POST'])
def deleteQuery():
    request_data = request.get_json()
    tableName = request_data["tableName"]
    conditionColumn = request_data["columnName"]
    conditionValue = request_data["columnValue"]

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)
    if metaData:
        availableColumns = list(metaData["columns"].keys())

        if conditionColumn not in availableColumns:
            return "Invalid condition column name: " + conditionColumn
        else:
            conditionIndex = availableColumns.index(conditionColumn)

        data = list(
            filter(lambda row: row[conditionIndex] != conditionValue, data))
        print(len(data))
        dataToRaw(tableName, data)
    else:
        return "Table not found with name: " + tableName

    return "dummy"


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


# rowList = rawToData("customer1")
# print(rowList)
# rowList[0][0] = "nikunj"
# # for rowIndex in range(len(rowList)):
# #     rowList[rowIndex] = ",".join(rowList[rowIndex])
# # rowList = "|".join(rowList)
# dataToRaw("customer1", rowList)
# print(rowList)
app.run(debug=True)
