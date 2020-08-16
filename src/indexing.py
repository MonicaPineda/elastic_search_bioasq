import elasticsearch
import xml.etree.cElementTree as ET 
import datetime
from elasticsearch import Elasticsearch
import boto3
import sys,os,os.path
import xml.etree.cElementTree as ET # C implementation of ElementTree
import time
import gzip
import multiprocessing
from multiprocessing import Pool
from pubmed_paper import Pubmed_paper
import argparse

processed_files=[]

def create_pubmed_paper_index(es, index_name, delete=False):    
    settings = {
        # changing the number of shards after the fact is not 
        # possible max Gb per shard should be 30Gb, replicas can 
        # be produced anytime
        # https://qbox.io/blog/optimizing-elasticsearch-how-many-shards-per-index
        "number_of_shards" : 5,
        "number_of_replicas": 0
    }
    mappings = {
        "pubmed-paper": {
            "properties" : {
                "title": { "type": "text", "analyzer": "standard"},
                "abstract": { "type": "text", "analyzer": "standard"},
                "mesh": { "type": "text", "analyzer": "standard"},
                "keywords" : { "type": "text", "analyzer": "standard"},
                "created_date": {
                    "type":   "date",
                    "format": "yyyy-MM-dd"
                }
            }
        }
    }
    if delete:
        es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, 
                    body={ 'settings': settings,
                            'mappings': mappings }, 
                    request_timeout=30)
    return 


def process_single_file(es, single_file, tag, zipped=True):
    i = single_file[0]
    f = single_file[1]
    print(i,f)
    print("Read file %d filename = %s" % (i, f))
    time0 = time.time()
    time1 = time.time()
    if zipped:
        inF = gzip.open(f, 'rb')
    else:
        inF = open(f, 'r')
    # we have to iterate through the subtrees, ET.parse() would result
    # in memory issues
    context = ET.iterparse(inF, events=("start", "end"))
    # turn it into an iterator
    context = iter(context)

    # get the root element
    event, root = context.__next__()
    print("Preparing the file: %0.4fsec" % ((time.time() - time1)))
    time1 = time.time()

    documents = []
    time1 = time.time()
    for event, elem in context:
        if event == "end" and elem.tag == tag['article_tag']:
            doc, source = extract_data(es, elem, tag)
            documents.append(doc)
            documents.append(source)
            elem.clear()
    root.clear()
    print("Extracting the file information: %0.4fsec" % 
        ((time.time() - time1)))
    time1 = time.time()

    res = es.bulk(index=index_name, body=documents, request_timeout=300)
    print("Indexing data: %0.4fsec" % ((time.time() - time1)))
    print("Total time spend on this file: %0.4fsec\n" % 
        ((time.time() - time0)))
    processed_files.extend([f])
    time.sleep(10)


def fill_pubmed_papers_table(es, list_of_files, tag, zipped=True):
    # Loop over all files, extract the information and index in bulk
    for i, f in enumerate(list_of_files):
        process_single_file(es, (i, f), tag, zipped=True)
    #pool = Pool(processes=multiprocessing.cpu_count())
    #pool = Pool(processes=2)
    #pool.map(process_single_file, [(i,f) for i, f in enumerate(list_of_files)])
    return 


def extract_data(es, citation, tag):
    new_pubmed_paper = Pubmed_paper()
    if citation.tag == 'PubmedArticle':
        citation = citation.find(tag['citation_tag'])

    new_pubmed_paper.pm_id = citation.find(tag['pmid_tag']).text
    new_pubmed_paper.title = citation.find(tag['title_tag']).text
    
    #Add abstract data
    Abstract = citation.find(tag['abstract_tag'])
    if Abstract is not None:
        # Here we discart information about objectives, design, 
        # results and conclusion etc.
        for text in Abstract.findall(tag['abstract_text_tag']):
            if text.text:
                if text.get(tag['label_tag']):
                    new_pubmed_paper.abstract += '<b>' + text.get(tag['label_tag']) + '</b>: '
                new_pubmed_paper.abstract += text.text + '<br>'
    
    #Add mesh data
    MeshHeadingList = citation.find(tag['mesh_list_tag'])
    if MeshHeadingList is not None:
        # Here we discart information about objectives, design, 
        # results and conclusion etc.
        for text in MeshHeadingList.findall(tag['meshheading_tag']):
            description_mesh = text.find( tag['descriptionname_tag'] )
            if description_mesh.text:
                new_pubmed_paper.mesh += description_mesh.text + ' '
    

    DateCreated = citation.find(tag['created_date_tag'])
    new_pubmed_paper.created_datetime = datetime.datetime(
        int(DateCreated.find(tag['created_year_tag']).text),
        int(DateCreated.find(tag['created_month_tag']).text),
        int(DateCreated.find(tag['created_day_tag']).text)
    )
    
    #Add keywords
    KeywordsList = citation.find(tag['keywords_list_tag'])
    if KeywordsList is not None:
        # Here we discart information about objectives, design, 
        # results and conclusion etc.
        for text in KeywordsList.findall(tag['keyword_tag']):
            if text.text:
                new_pubmed_paper.keywords += text.text

    doc, source = get_es_docs(new_pubmed_paper)
    del new_pubmed_paper
    return doc, source


def get_es_docs(paper):
    source = {
        'title': paper.title,
        'created_date': paper.created_datetime.date(),
        'abstract': paper.abstract,
        'mesh': paper.mesh,
        'keywords' : paper.keywords
    }
    doc = {
        "index": {
            "_index": index_name,
            "_type": type_name,
            "_id": paper.pm_id
        }
    }
    return doc, source

def get_doc(low_date, up_date, list_of_terms=[]):
    term_doc = []
    for sub_string in list_of_terms:
        term_doc.append({
            "match_phrase":{
                "title": sub_string
            }
        })
        term_doc.append({
            "match_phrase":{
                "abstract": sub_string
            }
        })
    doc = {
        "query": {
            "bool": {
                "must": [{
                    "range": {
                        "created_date":{
                            "gte" : low_date.strftime('%Y-%m-%d'), 
                            "lte" : up_date.strftime('%Y-%m-%d'), 
                            "format": "yyyy-MM-dd"
                        }
                    }
                },
                {
                    'bool': {
                        "should": term_doc
                    }
                }]
            }
        }
    }
    return doc



def process_pubmed(es, index_name, baseline_path, tag, create_index=True):
    if create_index:
        print('Create pubmed paper index: ',index_name)
        create_pubmed_paper_index(es, index_name)
        print('done')
    else:
        print('Index already created: ',index_name)
    
    # fill pubmed papers index
    exclude_articles = []
    pubmed_folder = baseline_path
    # get a list of all .gz files in this folder
    list_of_files = [os.path.join(pubmed_folder, f) for f in os.listdir(pubmed_folder) \
                    if os.path.isfile(os.path.join(pubmed_folder, f)) ]
    
    list_of_files_cleaned = []
    for file_s in list_of_files:
        if file_s not in exclude_articles:
            list_of_files_cleaned += [file_s]
        else:
            print('removed ',file_s)
    
    print("Indexing ",len(list_of_files_cleaned),' files')
    fill_pubmed_papers_table(es, list_of_files_cleaned, tag, zipped=True)
    
    with open('index_bioasq2018.txt','w+') as f:
        f.write(",".join(map(str, list_of_files_cleaned)))
    
    print('Finish indexing ',len(list_of_files_cleaned),' files.')
    print(list_of_files_cleaned)        
    return 

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index generator")
    parser.add_argument("-n", "--name", dest="n", type=str,
                        help="index_name")
    parser.add_argument("-p", "--path", dest="p", type=str,
                        help="data path")
    args = parser.parse_args()
    index_name= args.n
    baseline_path= args.p
    es = Elasticsearch(hosts=['http://localhost:9200'])
    es.indices.delete(index=index_name, ignore=[400, 404])
    type_name = 'pubmed-paper'
    tag = {
        'article_tag': 'PubmedArticle',
        'citation_tag': 'MedlineCitation',
        'pmid_tag': 'PMID',
        'title_tag': 'Article/ArticleTitle',
        'abstract_tag': 'Article/Abstract', 
        'abstract_text_tag': 'AbstractText', 
        'label_tag': 'Label', 
        'created_date_tag': 'DateRevised',
        'created_year_tag': 'Year',
        'created_month_tag': 'Month',
        'created_day_tag': 'Day',
        'mesh_list_tag': 'MeshHeadingList',
        'meshheading_tag': 'MeshHeading',
        'descriptionname_tag':  'DescriptorName',
        'keywords_list_tag' : 'KeywordList',
        'keyword_tag' : 'Keyword'
    }
    process_pubmed(es, index_name, baseline_path, tag, create_index=True)
