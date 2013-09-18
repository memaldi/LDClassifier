import json
import sys
import urllib
import urllib2
import time

result = 0
end = False
it = 1
offset = 0

sparql_endpoint = sys.argv[1]
limit = int(sys.argv[2])
output = sys.argv[3]

f = open(output, 'w')

while not end:
    query = 'SELECT DISTINCT * WHERE { ?s ?p ?o } OFFSET %s LIMIT %s' % (str(offset), str(limit))
    #params = {'query': query, 'output': 'json', 'format': 'application/sparql-results+json'}
    #headers = {'Accept': 'application/json, application/sparql-results+json'}
    #self.sparql_http_connection.request('GET', self.route + '?' + urllib.urlencode(params))
    #response = self.sparql_http_connection.getresponse()
    params = urllib.urlencode({'query': query, 'output': 'json', 'format': 'application/sparql-results+json'})
    request = urllib2.Request(sparql_endpoint + '?' + params)
    request.add_header('Accept', 'application/rdf+xml, application/sparql-results+json')
    retries = 1
    success = False
    while not success:
        try:
            response = urllib2.urlopen(request)
            success = True
        except Exception as e:
            wait = retries * 30;
            print 'Error! Waiting %s secs and re-trying...' % wait
            sys.stdout.flush()
            time.sleep(wait)
            retries += 1
            #response = urllib2.urlopen(request)

    if response.code == 200:
        data = response.read()
        try:
            json_data = json.loads(data)
        except Exception as e:
            print 'Error reading JSON!'
            print data
        if len(json_data['results']['bindings']) > 0:
            for json_triple in json_data['results']['bindings']:
                subject = json_triple['s']['value']
                predicate = json_triple['p']['value']
                if json_triple['o']['type'] == "literal":
                    object = '"%s"' % json_triple['o']['value']
                else:
                    object =  '<%s>' % json_triple['o']['value']
                output_str = '<%s> <%s> %s\n' % (subject, predicate, object)
                f.write(output_str.encode('utf-8'))
        else:
            end = True
    if (offset + limit) % 1000 == 0:
        print 'Writed %s triples...' % (offset + limit)
    if (offset + limit) % 10000 == 0:
        print 'Wating 30 secs...'
        sys.stdout.flush()
        time.sleep(30)
        print 'Ready!'
    offset = (limit * it)
    it = it + 1

    sys.stdout.flush()

f.close()
print 'End!'
