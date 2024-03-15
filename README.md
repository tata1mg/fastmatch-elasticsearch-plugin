Match a set of integers with the large lists of integers stored in ElasticSearch (as serialized RoaringBitmap)
===========================================

This filter plugin uses RoaringBitmap to allow efficient matching of a set of integers with a large interger list stored 
in ES as serialized RoaringBitmap. 

A sample example using python - 


```
from pyroaring import BitMap
import base64

from elasticsearch import Elasticsearch

if __name__ == "__main__":
    es = Elasticsearch(hosts=["http://localhost:9200"])
    # in the real world, the bitmap serialization would be for a big list
    # and you could even compute them before and store them for later user
    
    bm = BitMap([110001, 110002])

    # this will match "tags" field with the given keyword
    # but only for documents that have a any of the [110001, 110002] in its locations.
    result = es.search(
        index="test_index",
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "tags": "<tag-keyword>"
                            }
                        }
                    ],
                    "filter": {
                        "script": {
                            "script": {
                                "source": "fast_matcher",
                                "lang": "fast_matcher",
                                "params": {
                                    "field": "locations",
                                    "operation": "include",
                                    "terms": base64.b64encode(BitMap.serialize(bm)).decode("utf-8")
                                }
                            }
                        }
                    }
                }
            }
        }
    )
    print(result)

```

