from flask import Flask, request
import flask
import json

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
            newLine = tableName + "-->" + rowList
            newContent += newLine
        else:
            newContent += line
    print(newContent)
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
    return "Hello World!"


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

    return "Table Created Successfully"


@app.route("/update", methods=['POST'])
def updateQuery():
    request_data = request.get_json()
    tableName = request_data["table_name"]
    columnList = request_data["column_list"]
    condition = request_data["condition"]

    metaData = isTableExist(tableName)

    if metaData:
        columnIndexes = {}
        for column in columnList.keys():
            print("column", column)
            index = verifyColumn(column.strip(" "), metaData)
            if index == -1:
                return "Invalid Column Name: " + column
            columnIndexes[index] = columnList[column]

        print(columnIndexes)

        comparatorIndex = parseConditionIndex(condition, metaData)

        if comparatorIndex != -1:
            comparatorValue = condition.split("=")[1].strip(" ")
            file = open("db1.txt")
            content = ""
            for line in file:
                existingTable = line.split("-->")[0]
                if existingTable == tableName:
                    rowList = line.split("-->")[1].split("|")
                    for rowIndex in range(len(rowList)):
                        values = rowList[rowIndex].split(",")
                        if values[comparatorIndex] == comparatorValue:
                            for index in columnIndexes.keys():
                                values[index] = columnIndexes[index]
                                print("values", values)
                        rowList[rowIndex] = ",".join(values)
                    rowList = "|".join(rowList)
                    newLine = tableName + "-->" + rowList
                    content += newLine
                    print("content", content)
                    continue
                content += line
                print("content", content)
            file.close()
            file = open("db1.txt", "w+")
            file.write(content)
        else:
            return "Condition is incorrect"
        return "Nothing"
    else:
        return "No table available with name: " + tableName


@app.route('/select', methods=['POST'])
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


@app.route('/insert', methods=['POST'])
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

    for row in data:
        if row[primaryKeyIndex] == columnList[primaryKeyIndex]:
            return "Primary key must be unique"

    data.append(columnList)
    print("data", data)
    dataToRaw(tableName, data)

    return "Record Inserted"


@app.route('/validate', methods=['POST'])
def isUserValid():
    isValid = False
    request_data = request.get_json()
    authentication = open('authentication.json')
    users = json.load(authentication)["users"]

    username = request_data["username"]
    password = request_data["password"]

    for user in users:
        print(user["username"])
        if(username == user["username"] and password == user["password"]):
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
