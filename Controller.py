import re
import requests
import json
import time
from prettytable import PrettyTable

INVALID_QUERY = "invalid query"
BUSY_STATE = "busy"

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
    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
        return "Condition is missing"

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
    if site_url:
        response = requests.post(site_url + "/update", json=data)
        printStateOfDatabase(site_url)
    else:
        return "ERROR -> No site url found for this Table Name: " + tableName
    return response.text


def selectQuery(query):
    matchGroups = re.match(
        "SELECT\s([\w\s,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?([\w\s]+=['\w\s]+)?", query)

    print(matchGroups.groups())

    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
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
        response = requests.post(site_url + "/select", json=data)
        data = json.loads(response.text)
        isFetched = data["isFetched"]
        if isFetched:
            table = PrettyTable(data["columnNames"])
            for row in data["columnValues"]:
                table.add_row(row)
            print("======================RESULT TABLE=====================")
            print(table)
            print("=======================================================\n")
            return data["msg"]
        else:
            return "ERROR -> No results found"
    else:
        return "ERROR -> No site url found for this Table Name: " + tableName


def insertQuery(query):
    matchGroups = re.match(
        "INSERT INTO ([A-Za-z0-9_]+)\sVALUES\s\(([a-z,A-Z0-9\\+\@\.\s']+)\)", query)
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

    if site_url:
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
    print(matchGroups.group(1))
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
    printStateOfDatabase(site_url)
    response = requests.post(site_url + "/delete", json=deletedata)
    return response.text


def getDump():
    userInput = readSiteInput()
    site_url = getSiteUrlByInput(userInput)
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


def addUserLog(userName, query, msg):
    file = open("userLog.txt", "a+")
    file.write("userName: " + userName + " Query: " +
               query + " Message: " + msg + "\n")
    file.close()


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
    print("         QUERY : " + query)
    print("Execution Time : " + str(executionTime) + " ns")
    print("  Query Status : " + msg)
    print("=======================================================")


db = {}

username = input("username: ")
password = input("password: ")

data = {
    "username": username,
    "password": password
}


response = requests.post(SITE1_URL + "/validate", json=data)

isValid = json.loads(response.text)["isValid"]

if isValid:
    query = "INSERT INTO grade VALUES (104, 5308, 2,'A+')"
    query1 = "DELETE FROM customer WHERE customer_name= 'Jemis2'"
    query2 = "UPDATE customer21 SET customer_name= 'hello',customer_address= 'Surat' WHERE customer_name='Jemis7'"
    query3 = "INSERT INTO course VALUES (5508, 'Cloud Computing', 5)"
    query4 = "SELECT * FROM course"
    query5 = "CREATE TABLE student (studentId string 25 PK, studentName string 25)"
    queryType = identifyQuery(query)
    if(queryType != INVALID_QUERY):
        startTime = time.time()
        processQuery = runParser(queryType, query)
        msg = processQuery()
        executionTime = time.time() - startTime
        printLog(query, msg, executionTime)
        addUserLog(username, query, msg)
    else:
        print("Invalid Query Type")
else:
    print("ERROR -> Invalid User: " + str(username))
