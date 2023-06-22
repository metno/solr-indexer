#!/usr/bin/env python3

import pysolr
from solrindexer.multithread.threads import concurrently
from concurrent import futures as Futures


def handleResults(doc):
    
    print(doc['id'])
        
        
def main():

    search_rows = 10
    search_start = 0
    
    solrcon = pysolr.Solr('http://metsis-solr.met.no:8983/solr/adc-d8-dev', 
                          always_commit=False, timeout=1020,
                          auth=None)

    results = solrcon.search('*:*', fq='collection:(NBS)', rows=0, start=0)
    print(results.hits)
    hits = results.hits

    while (search_start + search_rows) <= hits:
        print(search_start + search_rows)
        results = solrcon.search('*:*', fq='collection:(NBS)', 
                                 rows=search_rows, start=search_start)
        
        docs = list(results)
        newdocs = list()
        for (doc, newdoc) in concurrently(fn=handleResults, inputs=docs,
                                          max_concurrency=8):
            newdocs.append(newdoc)
        
        Futures.ALL_COMPLETED
        print(len(newdocs))
        # try:
        #     solrcon.add(newdocs)
        # except Exception as e:
        #     print("Error adding documents to Solr: %s", e)

        del docs
        del newdocs
        search_start += search_rows 
    
    
if __name__ == '__main__':
    main()
