import re
import requests
import json
import time
from prettytable import PrettyTable
from datetime import datetime
import localOperation

INVALID_QUERY = "invalid query"
BUSY_STATE = "busy"
LOCAL_DATABASE = "local_database"

SITE1_URL = "http://35.225.117.133"
SITE2_URL = "http://35.233.233.65"

LOCAL_URL = "http://127.0.0.1:5000"

supportedQuery = ["SELECT", "UPDATE", "CREATE", "DELETE", "INSERT"]


def identifyQuery(query):
    if query.split()[0] in supportedQuery:
        return query.split()[0]
    else:
        return INVALID_QUERY


def updateQuery(query):
    matchGroups = re.match(
        "UPDATE\s([\w]+)\sSET\s([\w\s=,\@\.\\+'\"]+)\s?(WHERE)\s([\w\s]+=['\w\s]+)", query)

    if matchGroups == None:
        return "ERROR -> Update Query is invalid"

    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
        return "ERROR -> Update Query is invalid"

    tableName = matchGroups.group(1)
    columns = matchGroups.group(2)
    columnList = {}
    for column in columns.split(","):
        columnName = column.split("=")[0].strip(" ")
        columnValue = column.split("=")[1].replace("'", "").strip(" ")
        columnList[columnName] = columnValue

    if matchGroups.group(3) == "WHERE":
        condition = matchGroups.group(4)

    data = {
        "column_list": columnList,
        "table_name": tableName,
        "condition": condition
    }

    site_url = getSiteUrlByTableName(tableName)
    if site_url == LOCAL_DATABASE:
        msg = localOperation.updateQuery(tableName, columnList, condition)
        return msg
    elif site_url:
        response = requests.post(site_url + "/update", json=data)
        printStateOfDatabase(site_url)
    else:
        return "ERROR -> No site url found for this Table Name: " + tableName
    return response.text


def selectQuery(query):
    matchGroups = re.match(
        "SELECT\s([\w\s,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?([\w\s]+=['\w\s]+)?", query)

    if matchGroups.group(3) == "WHERE" and matchGroups.group(4) == None:
        return "Condition is missing"

    columnNames = matchGroups.group(1).split(",")
    for columnIndex in range(len(columnNames)):
        columnNames[columnIndex] = columnNames[columnIndex].strip(" ")
    tableName = matchGroups.group(2)
    condition = False
    if matchGroups.group(3) == "WHERE":
        condition = matchGroups.group(4)

    data = {
        "column_names": columnNames,
        "table_name": tableName,
        "condition": condition
    }

    site_url = getSiteUrlByTableName(tableName)
    if site_url:
        printStateOfDatabase(site_url)
        if site_url == LOCAL_DATABASE:
            data = localOperation.selectQuery(
                tableName, columnNames, condition)
        else:
            response = requests.post(site_url + "/select", json=data)
            data = json.loads(response.text)
        try:
            isFetched = data["isFetched"]
            if isFetched:
                table = PrettyTable(data["columnNames"])
                for row in data["columnValues"]:
                    table.add_row(row)
                print("======================RESULT TABLE=====================")
                print(table)
                print("=======================================================\n")
                return data["msg"]
            elif not isFetched:
                return "ERROR -> No results found"
        except:
            return response.text
    else:
        return "ERROR -> No site url found for this Table Name: " + tableName


def insertQuery(query):
    matchGroups = re.match(
        "INSERT INTO ([A-Za-z0-9_]+)\sVALUES\s\(([a-z,A-Z0-9\\+-\@\.\s']+)\)", query)
    tableName = matchGroups.group(1)
    columnValues = matchGroups.group(2)
    column_values = []

    for column in columnValues.split(","):
        column_values.append(column.replace("'", "").strip(" "))

    insertdata = {
        "table_name": tableName,
        "columnValues": column_values
    }

    site_url = getSiteUrlByTableName(tableName)

    if site_url == LOCAL_DATABASE:
        msg = localOperation.insertQuery(tableName, column_values)
        return msg
    elif site_url:
        response = requests.post(site_url + "/insert", json=insertdata)
        printStateOfDatabase(site_url)
    else:
        return "ERROR -> No site url found for this Table Name: " + tableName

    return response.text


def createQuery(query):
    matchGroups = re.match(
        "CREATE\sTABLE\s([\w]+)\s\(([a-zA-Z0-9_\s,]+)\)?", query)

    tableName = matchGroups.group(1)

    createData = {
        "tableName": tableName
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
    createData["query"] = query

    siteIndex = readSiteInput()

    site_url = getSiteUrlByInput(siteIndex)

    if site_url == LOCAL_DATABASE:
        response = localOperation.createTable(
            tableName, createData["primary_key"], columnMetas, query)
    else:
        response = requests.post(site_url + "/create", json=createData)
        response = json.loads(response.text)

    isTableCreated = response["isTableCreated"]
    msg = response["msg"]
    printStateOfDatabase(site_url)
    if isTableCreated:
        defineTableIntoSite(siteIndex, tableName)
        return msg
    else:
        return msg


def deleteQuery(query):
    matchGroups = re.match(
        "DELETE FROM ([\w]+) WHERE \s?([\w\s=,'\"]+)", query)
    tableName = matchGroups.group(1)
    condition = matchGroups.group(2)
    columnName = condition.split("=")[0].strip(" ")
    columnValue = condition.split("=")[1].strip(" ")

    deletedata = {
        "tableName": tableName,
        "columnName": columnName,
        "columnValue": columnValue
    }

    site_url = getSiteUrlByTableName(tableName)
    if site_url == LOCAL_DATABASE:
        msg = localOperation.deleteQuery(tableName, columnName, columnValue)
        return msg
    elif site_url:
        response = requests.post(site_url + "/delete", json=deletedata)
        return response.text
    else:
        print("ERROR -> No site url found for this table: " + tableName)

    printStateOfDatabase(site_url)


def getDump():
    userInput = readSiteInput()
    site_url = getSiteUrlByInput(userInput)
    if site_url == LOCAL_DATABASE:
        data = localOperation.getDump()
    else:
        response = requests.get(site_url + "/dump")
        data = json.loads(response.text)
    fileName = input("Enter file name for dump: ")
    if fileName == "":
        fileName = "dump.txt"
    file = open(fileName, "w+")
    file.write("".join(data))
    file.close()


def readSiteInput():
    try:
        gdd = open("GlobalDataDictionary.json")
        sites = json.load(gdd)["sites"]
        for siteIndex in range(len(sites)):
            print(str(siteIndex + 1) + ": " + sites[siteIndex]["site_url"])
        userInput = int(input("Enter site number: "))
        if userInput > len(sites) or userInput < 1:
            print("Enter site number between 1 to " + str(len(sites)))
            readSiteInput()
        return userInput
    except:
        print("Only Integer inputs are allowed")
        readSiteInput()
    finally:
        gdd.close()


def runParser(queryType, query):
    # "SELECT\s([\w,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?(\w+=*)?"
    switcher = {
        "SELECT": lambda: selectQuery(query),
        "UPDATE": lambda: updateQuery(query),
        "CREATE": lambda: createQuery(query),
        "INSERT": lambda: insertQuery(query),
        "DELETE": lambda: deleteQuery(query)
    }
    return switcher.get(queryType, INVALID_QUERY)


def printStateOfDatabase(siteUrl):
    if siteUrl == LOCAL_DATABASE:
        response = True
        data = localOperation.getStateOfDatabase()
    else:
        response = requests.get(siteUrl + "/state")
        data = json.loads(response.text)
    print()
    print("=======================EVENT LOG=======================")
    print("SITE URL: " + siteUrl + "\n")
    if response:
        table = PrettyTable(["Table Name", "Total Rows"])
        for row in data:
            table.add_row(row)
        print(table)
    else:
        "No data is available till now"
    print("=======================================================")
    print()


def defineTableIntoSite(input, tableName):
    try:
        gdd = open("GlobalDataDictionary.json")
        sites = json.load(gdd)["sites"]
        sites[input - 1]["tables"].append(tableName)
        data = {
            "sites": sites
        }
        gdd.close()
        with open("GlobalDataDictionary.json", "w") as gdd:
            json.dump(data, gdd)
    finally:
        gdd.close()


def getSiteUrlByInput(input):
    try:
        gdd = open("GlobalDataDictionary.json", "r")
        sites = json.load(gdd)["sites"]
        return sites[input-1]["site_url"]
    finally:
        gdd.close()


def addUserLog(query, msg):
    with open("userLog.txt", "a+") as file:
        file = open("userLog.txt", "a+")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        file.write(dt_string + " User Name: " + username + " Query: " +
                   query + " Message: " + msg + "\n")


def getSiteUrlByTableName(tableName):
    try:
        gdd = open("GlobalDataDictionary.json")
        sites = json.load(gdd)["sites"]
        for site in sites:
            if tableName in site["tables"]:
                return site["site_url"]
        return False
    finally:
        gdd.close()


def printLog(query, msg, executionTime):
    print("==========================LOG==========================")
    print("         Query : " + query)
    print("Execution Time : " + str(executionTime) + " ns")
    print("  Query Status : " + msg)
    print("=======================================================")


def executeQuery():
    query = input("Enter Query: ")
    queryType = identifyQuery(query.strip(" "))
    if(queryType != INVALID_QUERY):
        startTime = time.time()
        processQuery = runParser(queryType, query)
        msg = processQuery()
        executionTime = time.time() - startTime
        printLog(query, msg, executionTime)
        addUserLog(query, msg)
    else:
        print("Invalid Query Type")


def defineCardinality():
    while True:
        print("1. 1 -> 1\n2. 1 -> M")
        try:
            userInput = int(input("Enter Cardinality Type"))
            if userInput > 2 or userInput < 1:
                print("Please enter 1 or 2 to choose ")
            else:
                return "(1->1)" if userInput == 1 else "(1->M)"
        except:
            continue


def createRelationShips():
    currentTable = input("Enter Current Table: ")
    referenceTable = input("Enter Reference Table: ")
    isCurrentTableValid = False
    isReferencedTableValid = False
    with open("GlobalDataDictionary.json", "r") as file:
        sites = json.load(file)["sites"]
        for site in sites:
            if currentTable in site["tables"]:
                isCurrentTableValid = True
            if referenceTable in site["tables"]:
                isReferencedTableValid = True
    if not isCurrentTableValid:
        return "Current table is not present in Database"
    if not isReferencedTableValid:
        return "Referenced table is not present in database"
    cardinality = defineCardinality()
    relationship = input("Enter Relationship Name: ")
    with open("entityrelationship.txt", "a+") as file:
        file.write(currentTable + "---" + cardinality + "---" +
                   relationship + "---" + cardinality + "---" + referenceTable)
        file.write("\n")


def printERD():
    print("======================ER========================")
    with open("entityrelationship.txt", "r") as file:
        for line in file:
            if line == "" or line == "\n":
                print("No relationships are available")
            else:
                print(line)
    print("================================================\n")


def actionSwitcher(userInput):
    userInput = str(userInput)
    switcher = {
        "1": lambda: executeQuery(),
        "2": lambda: getDump(),
        "3": lambda: createRelationShips(),
        "4": lambda: printERD(),
        "5": lambda: exit()
    }
    return switcher.get(userInput, "Please enter input in between 1-4")


username = input("username: ")
password = input("password: ")

data = {
    "username": username,
    "password": password
}

response = requests.post(SITE1_URL + "/validate", json=data)

isValid = json.loads(response.text)["isValid"]

if isValid:
    while True:
        print("1. Execute Query\n2. Create Dump\n3. Create Relationship\n4. Print E-R\n5. Exit")
        try:
            userInput = int(input("Enter Action Number: "))
            action = actionSwitcher(userInput)
            action()
        except:
            print("Invalid Input")
            continue
else:
    print("ERROR -> Invalid User: " + str(username))
