# from bottle import route, run, request, post
from bottle import *
import json
import os
import datetime, time

# sys.path.insert(1, './dedupes')
PATH = os.path.dirname(__file__)
sys.path.insert(1, PATH) 
# sys.path.insert(1, './') 
err = {'code': '', 'text': '', 'details': ''}
error = err

@route('/ver')
@route('/version')
@route('/ver<ver:int>')
def version(ver=1):
    # response.content_type = 'text/html' #'text/plain'
    assert int(ver) >= 0, 'Negative value "version"'

    if ver in [0, 1]:
        response.status = 200
        response.set_cookie(name='APIver', value=str(ver), path='/',  max_age=100500)  #expires=int(time.time()) + 3600)
        # print(request.get_cookie('APIver', '0'))
        msg = {"status" : 200,  # response.status,
               "message": "Deduper service API version: %s" % ver,
               "details": getUrl(request),  #request.environ['PATH_INFO'],
               "data"   : ver
              }
        return json.dumps(msg)
    else:
        err['code'] = 5001 
        err['text'] = 'Wrong Deduper service API version: %s' % ver
        err['details'] = getUrl(request)

        return errorInternalServer(err)
        
@route('/')
@route('/main')
@route('/<page>') #:re:(\s)>') # \d+(%s\d)*
def main(page='main.html'):
    pageFile = PATH + 'www/' + page
    
    if os.path.exists(pageFile):
        with open(pageFile) as df :
            html = df.read()
            
            response.content_type = 'text/html'  # text/xml
            response.status = 200

            return html

    err['code'] = 5009
    err['text'] = 'Couldn\'t find file: "%s"' % page
    err['details'] = getUrl(request)

    return errorInternalServer(err)

@post('/v1/upload')  #, method='POST')
def control():
    isUploaded, result = uploadFile()       # upload file return filePath or Error
    msg = {}
    
    if isUploaded:  #if fileName !='' and fileName is not None:
        filePath = result
        fileName, fileExt = os.path.splitext(os.path.basename(filePath))
        
        if fileName=='' or fileName is None:
            assert fileName=='' or fileName is None, 'Empty "fileName"'
        else:
            pass #print('@@', fileName)
        
        if fileName !='' and fileName is not None:
            tableName = '%s_%s' % (cleanStringVar(fileName), cleanStringVar(str(datetime.date.today())))
            # msg = 'File was successfully saved to "%s".' % filePath

            response.set_cookie(name='file_name', value=str(fileName), path='/',  max_age=100500)#, expires=(int(time.time()) + 3600))
            response.set_cookie(name='file_path', value=str(filePath), path='/',  max_age=100500)            
            # print(request.get_cookie('file_name', ':('))
        else:
            err['code'] = 5004
            err['text'] = 'Undefined File name for: "%s"' % filePath # File extension not allowed.
            err['details'] = getUrl(request)

            return errorInternalServer(err)
        
        # start Parser
        msg = json.loads(parse(filePath, tableName)) # "Parse error: file wasn't validated."
        if msg['status'] != 200:
            return msg
        
        if uploadedRecCount == 1:
            response.content_type = 'application/json'
            response.status = 200        
            msg = {"status"  : 200,
                   "code"    : 2001,
                   "message" : "Data file contains only one record. Analysis wasn't finished.", 
                   "details" : getUrl(request),
                   "data"    : 1,
                   "dupes"   : 0
                  }            
            return json.dumps(msg)
        
        # run analysis
        msg = json.loads(runAnalysis(tableName))
        if msg['status'] != 200:
            return msg

        # export dupes data
        msg = exporData(tableName)
        # print(data)
        # msg = json.loads(data)
        # msg = data #json.loads(data)
        if msg['status'] != 200:
            return msg
        
        response.content_type = 'application/json'
        response.status = 200
        msg = {"status"  : 200,
               "code"    : 200,
               "message" : "File analysis was finished successfully.", #"File was uploaded into Database successfully.",
               "details" : getUrl(request),
               "data"    : msg['data'],
               "dupes"   : msg['dupes']
              }
        return json.dumps(msg)
    else:
        return result

@post('/v1/training') #, method='POST')
def activeLearning():
    isUploaded, result = uploadFile()       # upload file return filePath or Error
    msg = {}
    
    if isUploaded: #if fileName !='' and fileName is not None:
        filePath = result
        fileName, fileExt = os.path.splitext(os.path.basename(filePath))
        
        if fileName=='' or fileName is None:
            assert fileName=='' or fileName is None, 'Empty "fileName"'
        else:
            pass #print('@@', fileName)
        
        if fileName !='' and fileName is not None:
            tableName = '%s_%s' % (cleanStringVar(fileName), cleanStringVar(str(datetime.date.today())))
            # msg = 'File was successfully saved to "%s".' % filePath

            response.set_cookie(name='file_name', value=str(fileName), path='/',  max_age=100500)#, expires=(int(time.time()) + 3600))
            response.set_cookie(name='file_path', value=str(filePath), path='/',  max_age=100500)            
            # print(request.get_cookie('file_name', ':('))
        else:
            err['code'] = 5004
            err['text'] = 'Undefined File name for: "%s"' % filePath # File extension not allowed.
            err['details'] = getUrl(request)

            return errorInternalServer(err)
        
        # start Parser
        msg = json.loads(parse(filePath, tableName)) # "Parse error: file wasn't validated."
        if msg['status'] != 200:
            return msg
            
        # run analysis
        msg = json.loads(runAnalysis(tableName, True))
        if msg['status'] != 200:
            return msg

        # export dupes data
        msg = exporData(tableName)
        if msg['status'] != 200:
            return msg
        
        response.content_type = 'application/json'
        response.status = 200
        msg = {"status"  : 200,
               "code"    : 200,
               "message" : "File analysis was finished successfully.", #"File was uploaded into Database successfully.",
               "details" : getUrl(request),
               "data"    : msg['data'],
               "dupes"   : msg['dupes']
              }
        return json.dumps(msg)
    else:
        return result
        
@route('/v1/parse')
@route('/v1/parse/<tableName>')
def parse(filePath, tableName=''):
    # assert filePath is None, 'Empty "filePath"'

    if tableName == '':
        fileName = request.get_cookie('file_name') or os.path.splitext(os.path.basename(filePath))[0] # Get fileName from cookie or filePath #, secret='some-secret-key')

        if fileName !='' and fileName is not None:
            tableName = '%s_%s' % (cleanStringVar(fileName), cleanStringVar(str(datetime.date.today()))) #.replace('-', ''))
        else:
            err['code'] = 5005
            err['text'] = 'Undefined Table name.' # File extension not allowed.
            err['details'] = getUrl(request)
            return errorInternalServer(err)

    import parser as parser
    global pars

    parse = parser.parser(inFile= filePath, tabName =tableName)# =    # Initiate Parser
    # with 
    if parse is not None:
        isParsed = parse.startParse()
        if isParsed:
            global uploadedRecCount
            uploadedRecCount = parse.recCount
            # msg = '%s records were uploaded' % uploadedRecCount

            response.content_type = 'application/json'
            response.status = 200

            return json.dumps({"status"  : 200,
                               "code"    : 200,
                               "message" : '%s records were uploaded' % uploadedRecCount,
                               "details" : getUrl(request)
                              })
    err['code'] = 5006
    err['text'] = "File '%s' into table, parsing was failed." % (filePath) #, tableName)
    err['details'] = getUrl(request)
    return errorInternalServer(err)

@route('/v1/analyze')
@route('/v1/analyze/<tableName>')
def runAnalysis(tableName='', isActiveLearning = False):
    if tableName == '':
        fileName = request.get_cookie('file_name') #, secret='some-secret-key')
        if fileName !='' and fileName is not None:
            tableName = '%s_%s' % (cleanStringVar(fileName), cleanStringVar(str(datetime.date.today()))) #.replace('-', ''))
        else:
            err['code'] = 5005
            err['text'] = 'Undefined Table name.' # File extension not allowed.
            err['details'] = getUrl(request)
            return errorInternalServer(err)

    # from dedupes 
    import analyzer
    global analyzer
    analyze = analyzer.analyzer(tabName = tableName)
    if analyze is not None:
        isAnalysed, errMsg = analyze.startAnalyze(isActiveLearning) #True) # set True to run analysis with training

        if isAnalysed:
            uploadedRecCount = analyze.recordsCount
            dupesCount = analyze.dupesCount
            
            response.content_type = 'application/json'
            response.status = 200

            return json.dumps({"status"  : 200,
                               "code"    : 200,
                               "message" : '%s dupes were found within %s records' % (dupesCount, uploadedRecCount),
                               "details" : getUrl(request)
                              })
    err['code'] = 5007
    err['text'] = 'Unexpected error during table "%s" analysis. Details: %s' % (tableName, errMsg)
    err['details'] = getUrl(request)
    
    return errorInternalServer(err)

@route('/v1/export')
@route('/v1/export/<tableName>')
@route('/v1/export/<tableName>/<savefile>')
def exporData(tableName='', savefile = False):
    if tableName == '':
        fileName = request.get_cookie('file_name') #, secret='some-secret-key')
        if fileName !='' and fileName is not None and fileName is not None:
            # print(cleanStringVar(str(datetime.date.today())))
            tableName = '%s_%s' % (cleanStringVar(fileName), cleanStringVar(str(datetime.date.today()))) #.replace('-', ''))
        else:
            err['code'] = 5005
            err['text'] = 'Undefined Table name.' # File extension not allowed.
            err['details'] = getUrl(request)
            return errorInternalServer(err)
        
    # from dedupes 
    import export as export
    global export
    outFile = 'tmp/%s.xml' % tableName
    exporter = export.export(tabName = tableName, outFile = outFile)
    exporter.constraint = ' WHERE %(id)s NOT IN (SELECT %(id)s FROM %(tabName)s_entity_map)' % {'id': exporter.tablePK, 'tabName': exporter.tableName}
    all_data = exporter.xmlOriginalExport() #exporter.jsonExport()
    dupes = exporter.jsonDupesExport()
    
    if savefile:
        exporter.exportIntoXmlFile()
    
    if all_data is not None:
        response.content_type = 'application/json'
        response.status = 200

        msg = {"status"  : 200,
                "code"    : 200,
                "message" : '%s records were processed.' % (exporter.recCount),
                "details" : getUrl(request)}
        msg.update({"data": all_data})
        msg.update({"dupes": dupes})
        
        return msg #json.dumps(data)
                           # "data"    : data

    err['code'] = 5008
    err['text'] = 'Unexpected error during data export from table "%s".' % (tableName)
    err['details'] = getUrl(request)
    return errorInternalServer(err)

@route('/stop')
def stop(name="Stop service"):
    import signal
    response.content_type = 'application/json'
    response.status = 200

    os.system("kill %s &" % os.getpid()) # '&' in the end makes command call asynchronous
    return json.dumps({"status"  : 200,
                       "code"    : 200,
                       "message" : "Service was stopped successfully.",
                       "details" : getUrl(request)
                      })

def uploadFile(fieldName = 'file'):
    inFile = request.files.get(fieldName)                   # read field from field by name
    fileName, fileExt = os.path.splitext(inFile.filename)   # split file name and extention

    if fileExt not in ('.xml','.json','.csv'):              # check if file has right extention
        err['code'] = 5002 
        err['text'] = 'Unsupported File type: "%s"' % fileExt # File extension not allowed.
        err['details'] = getUrl(request)
        return False, errorUnsupportedMediaType(err) 

    savePath = os.path.abspath(PATH) +'/uploaded/'

    if not os.path.exists(savePath):
        os.makedirs(savePath)

    filePath = "%s%s%s" % (savePath, fileName, fileExt)
    assert filePath , 'Undefined "filePath"'  #is None or filePath ==''

    inFile.save(filePath, overwrite=True, chunk_size=262144) #256kb 65536)
    if os.path.isfile(filePath):
        return True, filePath

    # If case file save error
    err['code'] = 5003
    err['text'] = 'Unable to save the File: "%s"' % filePath # File extension not allowed.
    err['details'] = getUrl(request)
    return False, errorInternalServer(err)
            
def errorUnsupportedMediaType(error):
    status = 415
    response.content_type = 'application/json'
    response.status = status

    msg = {"status"  : status,
           "code"    : error.get('code', status),
           "message" : error.get('text', 'Unsupported Media Type'),
           "details" : error.get('details', getUrl(request))
          }
    return json.dumps(msg)

def errorInternalServer(error):
    status = 500
    response.content_type = 'application/json'
    response.status = status

    msg = {"status"  : status,
           "code"    : error.get('code', status),
           "message" : error.get('text', 'Internal Server Error'),
           "details" : error.get('details', getUrl(request))
          }
    # print('return Error: ', error['text'] )      
    return json.dumps(msg) # 

def getUrl(request):
    env = request.environ
    url = env['wsgi.url_scheme']+'://'
    
    if request.environ.get('HTTP_HOST'):
        url += env['HTTP_HOST']
    else:
        url += env['SERVER_NAME']
        
        if env['wsgi.url_scheme'] == 'https':
            if env['SERVER_PORT'] != '443':
               url += ':' + env['SERVER_PORT']
        else:
            if env['SERVER_PORT'] != '80':
               url += ':' + env['SERVER_PORT']

    url += env['PATH_INFO']
    return url

def cleanStringVar(var):
    return str(var).replace(' ', '').replace('(', '_').replace(')', '').replace('[', '_').replace(']', '').replace('.', '_').replace('-', '_').replace('_', '')

def start(isTest=False):
    import socket
    timeout = 360000    # Set TimeOut = 5 minutes for very Large files
    
    if isTest:
        host='45.55.160.201'
        port=8088
    else:
        host='127.0.0.1'
        port=8081
    
    socket.setdefaulttimeout(timeout)
    srv = run(debug=True, reloader=True, host=host, port=port)

if __name__ == "__main__":
    start()