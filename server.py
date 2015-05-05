from bottle import * # route, run
import json

@route('/')
@route('/hello/<name>')
def hello(name='World'):
    response.content_type = 'text/html' #'text/plain'
    response.status = 400
    return "Hello %s!" % name

@route('/v1/upload/', method='POST')
def control_upload():
    isUploaded, filePath = upload_file()
    if isUploaded:
        msg = "File successfully saved to '%s'." # % savePath
    else:
        msg = {"status"  : "500", 
               "message" : "File extension not allowed.",
               "details" : "http://localhost/upload"}
        response.status = 500
        
        return json.dumps(msg)
        
    # start Parser
    import parser13 as parser
    global parser
    parser = parser.start(filePath)
            
def upload_file():
        inFile = request.files.get('file')
        fileName, ext = os.path.splitext(inFile.filename)

        if ext not in ('.xml','.json','.csv'):
            return False, None # "File extension not allowed."

        savePath = os.path.abspath('.')+'/uploaded/'

        if not os.path.exists(savePath):
            os.makedirs(savePath)
        
        filePath = "%s/%s" % (savePath, fileName)

        inFile.save(filePath, overwrite=True, chunk_size=262144) #256kb 65536)
        return True, filePath #"File successfully saved to '{0}'.".format(savePath)
    
@route('/stop')
def stop(name="Stop service"):
    import os, signal
    response.content_type = 'application/json'
    response.status = 400
    return json.dumps({"status"  : "400", 
                       "message" : "Service was stopped successfully.",
                       "details" : "http://localhost/stop"})
    os.kill(os.getpid(), signal.SIGTERM)
    
def start():
    srv = run(host='45.55.160.201', debug=True, reloader=True, port=8081)

if __name__ == "__main__":
    start()
