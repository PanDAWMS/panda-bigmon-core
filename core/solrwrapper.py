import sunburnt
import datetime
#### Settings to be moved to the general settings file

solrconnectionURL = "http://aipanda087.cern.ch:8080/solr/"


filter_types = {
    'contains': u'%s',
    'startswith': u'%s*',
    'exact': u'%s',
    'gt': u'{%s TO *}',
    'gte': u'[%s TO *]',
    'lt': u'{* TO %s}',
    'lte': u'[* TO %s]',
}

#pandaid:[2475374667 TO 2575374667]

def parceQuery(query):

#    resp = si.query(author_t="martin")
    #price__range=(5, 7)
    # si.query(price__lt=7) price__any=True gt gte lt lte si.query(manufacturedate_dt__range=(datetime.datetime(2006, 1, 1), datetime.datetime(2006, 4, 1)) .sort_by("-price").sort_by("score")    
#    si.Q(si.Q("game") & ~si.Q(author_t="orson")

    expr=""
    for q in query:
        if q[-4:] == '__in':
            field = q[:-4]
            values = query[q]            
            tail = "("
            for value in values:
                if isinstance(value, (int, long, float, complex)):
                    tail += "si.Q("+field+'='+str(value) +') | '
                else:
                    tail += "si.Q("+field+'=\''+str(value) +'\') | '
            tail = tail[:-3]
            tail += ") &"
            expr = expr + tail
            
        else:        
            if str(query[q])[0] == '[':
                expr = expr + "si.Q("+str(q)+"=(" + str(query[q]) + ")) & "
            else:
                expr = expr + "si.Q("+str(q)+"='" + str(query[q]) + "') & "
    expr = expr[:-2]
    return expr

def makeQuerySolr(query, table, limit):
    si = sunburnt.SolrInterface(solrconnectionURL+table)
    expr = parceQuery(query)
    print expr
    print "Solr query started:", datetime.datetime.now().time()
    lenq = si.query(eval(expr)).paginate(start=0, rows=limit).execute()
    print  "Solr query finished:",datetime.datetime.now().time()
    return lenq

def addDocSolr(docs, table, maxAddBunchSize):
    si = sunburnt.SolrInterface(solrconnectionURL+table)
    for doc in docs:
        si.add(doc)
    si.commit()




