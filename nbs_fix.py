#!/usr/bin/env python3

import pysolr
from solrindexer.multithread.threads import concurrently
from concurrent import futures as Futures


def handleResults(doc):

    # print(doc['id'])
    if 'full_text' in doc:
        doc.pop('full_text')
    if 'bbox__maxX' in doc:
        doc.pop('bbox__maxX')
    if 'bbox__maxY' in doc:
        doc.pop('bbox__maxY')
    if 'bbox__minX' in doc:
        doc.pop('bbox__minX')
    if 'bbox__minY' in doc:
        doc.pop('bbox__minY')
    if 'bbox_rpt' in doc:
        doc.pop('bbox_rpt')
    if 'ss_access' in doc:
        doc.pop('ss_access')
    if '_version_' in doc:
        doc.pop('_version_')

    doc['isParent'] = False
    doc['isChild'] = False
    return doc


def main():

    search_rows = 1000
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
        # print(len(newdocs))
        try:
            solrcon.add(newdocs)
        except Exception as e:
            print("Error adding documents to Solr: %s", e)

        del docs
        del newdocs
        search_start += search_rows


if __name__ == '__main__':
    main()
