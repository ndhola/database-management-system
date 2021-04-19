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
    # print("newContent", newContent)
    file.close()
    file = open("db1.txt", "w+")
    file.write(newContent)

    return False


def createTable(tableName, primaryKey, columnMetas, query):
    columnMetas = "|".join(columnMetas)
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

    return {
        "msg": msg,
        "isTableCreated": isTableCreated
    }


def insertQuery(tableName, columnList):

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


def updateQuery(tableName, columnList, condition):

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


def selectQuery(tableName, columnList, condition):

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)

    if metaData:
        columnIndexes = []
        availabeColumns = list(metaData["columns"].keys())
        if columnList[0] != "*":
            for column in columnList:
                if column not in availabeColumns:
                    return "ERROR -> Invalid Column Name: " + column
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
                return "ERROR -> Invalid Condition Column: " + conditionColumn

            for columnIndex in range(len(availabeColumns)):
                if conditionColumn == availabeColumns[columnIndex]:
                    conditionIndex = columnIndex
        if not data:
            return "ERROR -> No Records for table name: " + tableName
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

    return data


def deleteQuery(tableName, conditionColumn, conditionValue):

    metaData = rawToMeta(tableName)
    data = rawToData(tableName)
    if metaData:
        availableColumns = list(metaData["columns"].keys())

        if conditionColumn not in availableColumns:
            return "ERROR -> Invalid condition column name: " + conditionColumn
        else:
            conditionIndex = availableColumns.index(conditionColumn)
        length = len(data)
        data = list(
            filter(lambda row: row[conditionIndex] != conditionValue, data))
        if length == len(data):
            return "ERROR -> No Records found where column name: " + conditionColumn + " with value: " + conditionValue
        else:
            dataToRaw(tableName, data)
    else:
        return "ERROR -> Table not found with name: " + tableName

    return "SUCCESS -> Record is deleted where column name: " + conditionColumn + " with value: " + conditionValue


def getDump():
    file = open("dump.txt")
    data = []
    for line in file:
        data.append(line)
    return data
