#!/usr/bin/env python3

import pysolr


def handleResults(doc):
    newdoc = doc
    # print(doc['id'])
    if 'full_text' in newdoc:
        newdoc.pop('full_text')
    if 'bbox__maxX' in newdoc:
        newdoc.pop('bbox__maxX')
    if 'bbox__maxY' in newdoc:
        newdoc.pop('bbox__maxY')
    if 'bbox__minX' in newdoc:
        newdoc.pop('bbox__minX')
    if 'bbox__minY' in newdoc:
        newdoc.pop('bbox__minY')
    if 'bbox_rpt' in newdoc:
        newdoc.pop('bbox_rpt')
    if 'ss_access' in newdoc:
        newdoc.pop('ss_access')
    if '_version_' in newdoc:
        newdoc.pop('_version_')

    newdoc.update({'isChild': False})
    newdoc.update({'isChild': False})

    return newdoc


def main():

    search_rows = 1000
    search_start = 0

    solrcon = pysolr.Solr('http://157.249.74.44:8983/solr/adc',
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
        # for (doc, newdoc) in concurrently(fn=handleResults, inputs=docs,
        #                                  max_concurrency=8):
        for doc in docs:
            newdoc = handleResults(doc)
            newdocs.append(newdoc)

        # Futures.ALL_COMPLETED
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
