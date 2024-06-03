def createDataCSV(filename, res):
    file = open(filename, 'wb')
    file.write(res.content)
    file.close()