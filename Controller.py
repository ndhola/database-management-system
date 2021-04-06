import re
import requests
import json
import time
from passlib.hash import bcrypt

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
        "UPDATE\s([\w]+)\sSET\s([\w\s=,'\"]+)\s?(WHERE)\s([\w]+=[\w]+)", query)
    if matchGroups.group(3) == "WHERE" and matchGroups.group(2) == None:
        return "Condition is missing"

    tableName = matchGroups.group(1)
    columns = matchGroups.group(2)
    columnList = {}
    for column in columns.split(","):
        columnName = column.split("=")[0].strip(" ")
        columnValue = column.split("=")[1].strip(" ")
        columnList[columnName] = columnValue

    if matchGroups.group(3) == "WHERE":
        condition = matchGroups.group(4)

    data = {
        "column_list": columnList,
        "table_name": tableName,
        "condition": condition
    }

    site_url = getSiteUrlByTableName(tableName)
    response = requests.post(site_url + "/update", json=data)
    return response.text


def selectQuery(query):
    matchGroups = re.match(
        "SELECT\s([\w\s,*\*?]+)\sFROM\s(\w*)\s?(WHERE)?\s?(\w+[=><]\w+)?", query)

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
    response = requests.post(site_url + "/select", json=data)
    return response.text


def insertQuery(query):
    matchGroups = re.match(
        "INSERT INTO ([A-Za-z0-9_]+)\sVALUES\s\(([a-z,A-Z0-9\@\.\s']+)\)", query)
    print(matchGroups.group(1))
    tableName = matchGroups.group(1)
    columnValues = matchGroups.group(2)

    print(columnValues)
    column_values = []
    for column in columnValues.split(","):
        column_values.append(column.strip(" "))
    print(column_values)
    insertdata = {
        "table_name": tableName,
        "columnValues": column_values
    }

    site_url = getSiteUrlByTableName(tableName)

    response = requests.post(site_url + "/insert", json=insertdata)

    printStateOfDatabase(site_url)
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
    response = requests.post(site_url + "/delete", json=deletedata)
    return response.text


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
    print("========EVENT LOG=========")
    print("SITE URL: " + siteUrl)
    if response:
        for row in data:
            print(row)
    else:
        "No data is available till now"
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


response = requests.post(LOCAL_URL + "/validate", json=data)

isValid = json.loads(response.text)["isValid"]

if isValid:
    query1 = "CREATE TABLE customer (customer_name string 25 PK, customer_address string 25)"
    query1 = "DELETE FROM student WHERE studentName= Andrew"
    query1 = "UPDATE customer17 SET customer_name= helly,customer_address= Surat WHERE customer_name=group2"
    query = "INSERT INTO customer VALUES (Jemis7, 140 Gautam Park)"
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
    print("ERROR: Invalid User")

# CREATE TABLE student (studentId string 25 PK, studentName string 25)
# CREATE TABLE faculty (facultyId int 25 PK, facultyName string 25, facultyEmail string 25)
# CREATE TABLE course (courseId int 25 PK, courseName string 25, courseRating string 25)
# CREATE TABLE grade (courseId int 25 PK, studentId int 25, grade string 25)

# INSERT INTO student VALUES (1, jemis, jemisgmailcom)
# INSERT INTO student VALUES (2, nikunj, nikunjgmailcom)
# INSERT INTO student VALUES (3, helly, hellygmailcom)
# INSERT INTO faculty VALUES (1, robert, robertgmailcom)
# INSERT INTO faculty VALUES (2, saurabh, saurabhgmailcom)
# INSERT INTO faculty VALUES (3, andrew, andrewgmailcom)


# SELECT * FROM student
# SELECT * FROM faculty

# UPDATE student SET studentName= Andrew,studentEmail= Andrewgmail WHERE studentId=1

# INSERT INTO course VALUES (1, Advance software development, 4)
# INSERT INTO course VALUES (2, Data warehouse, 4)
# INSERT INTO course VALUES (3, Communication skills, 5)


# INSERT INTO grade VALUES (101, 1, Advance software development, 4)
# INSERT INTO grade VALUES (102, 2, Data warehouse, 4)
# INSERT INTO grade VALUES (103, 3, Communication skills, 5)


# CREATE TABLE grade (gradeId int 25 PK, courseId int 25, studentId int 25, grade string 25)
