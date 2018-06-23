import requests
import mysql.connector
from datetime import datetime
from itertools import compress
import numpy as np
import urllib.parse


def fetch_query_results(cur, numrecords=10):
    result = []
    if type(numrecords) == int:
        i = 0
        for row in cur:    
            if i < numrecords:
                result.append(row)
                i += 1
    elif numrecords == 'all':
        for row in cur:    
            result.append(row)
    return result



def retrieve_journals():    
    cnx = mysql.connector.connect(user='root', password='',
                              host='localhost',
                              database='OA')
    curA = cnx.cursor()
    retrieve_journals_query = '''select name from doaj_journals order by doaj_seal desc, name asc;'''
    curA.execute(retrieve_journals_query)
    return fetch_query_results(curA, 'all')



def retrieve_article_bib_details(bibjson):
    try:
        pages = bibjson['start_page'] + '-' + bibjson['end_page']
    except:
        pages = ''
    try:
        title = str(bibjson['title'])
    except:
        title = ''
    try:
        year = bibjson['year']
    except:
        year = ''
    try:
        journal_title = bibjson['journal']['title']
    except:
        journal_title = ''
    try:
        journal_number = bibjson['journal']['number']
    except:
        journal_number = ''
    try:
        journal_volume = bibjson['journal']['volume']
    except:
        journal_volume = ''
    try:
        journal_publisher = bibjson['journal']['publisher']
    except:
        journal_publisher = ''
    try:
        link = bibjson['link'][0]['url']
    except:
        link = ''
    try:
        link_type = bibjson['link'][0]['type']
    except:
        link_type = ''
        
    authors = []
    try:
        for author in bibjson['author']:
            authors.append( author['name'] )
    except:
        pass
        
    subjects = []
    try:
        for subject in bibjson['subject']:
            subjects.append( subject['term'] )
    except:
        pass    

    return  {'title':title, 'pages':pages, 'year':year, 
             'journal_title':journal_title, 'journal_publisher': journal_publisher,
             'authors':str(authors), 'subjects':str(subjects)}



def retrieve_article_details(article):
    doaj_created_date = datetime.strptime( article['created_date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    doaj_last_updated = datetime.strptime( article['last_updated'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    article_details = {'id':article['id'], 
                       'doaj_created_date':doaj_created_date,
                      'doaj_last_updated':doaj_last_updated}
    return { **article_details, **retrieve_article_bib_details(article['bibjson']) }


def get_page_articles(page):
    try:
        print( 'Retrieving articles from page %s' % page['page'] )
        page_articles = []
        for article in page['results']:
            page_articles.append( retrieve_article_details( article ) )
        return page_articles
    except:
        pass

    
def get_data(journal_url):
    page = requests.get(journal_url).json()
    articles = get_page_articles(page)
    more_pages = True
    while more_pages:
        try:
            page = requests.get( page['next'] , timeout=None).json()
            articles.extend( get_page_articles(page) )
        except:
            more_pages = False
    return articles



def write_articles_to_doaj_articles_table(articles):
    
    cnx = mysql.connector.connect(user='root', password=,
                              host='localhost', charset = 'utf8mb4', collation='utf8mb4_unicode_ci',
                              database='OA')
    for article in articles:
        column_names        = article.keys()
        column_non_missing  = [] 
        for value in article.values():
            column_non_missing.append( value != '')

        non_missing_columns = list(compress(column_names, column_non_missing))
        non_missing_data    = list(compress(article.values(), column_non_missing))

        # concatenate into single string all the column names
        col_name_tuple = "("
        for column in non_missing_columns:
            col_name_tuple += column + ', '
        col_name_tuple = col_name_tuple[:-2] + ')'

        query = '''
        insert into doaj_articles %s
        values %s
        ''' % ( col_name_tuple, str(tuple(non_missing_data)) )

        cnx.cmd_query(query)
        cnx.commit()
    cnx.close()
    
    
    
def create_doaj_articles_table():
    query = '''
    create table doaj_articles
    (
    %s varchar(300) CHARACTER SET utf8,

    %s DATETIME,
    %s DATETIME,

    %s text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,

    %s varchar(300) CHARACTER SET utf8,
    %s varchar(300) CHARACTER SET utf8,
    %s varchar(300) CHARACTER SET utf8,
    %s varchar(300) CHARACTER SET utf8,

    %s text CHARACTER SET utf8,
    %s text CHARACTER SET utf8
    );
    ''' % tuple(['id', 'doaj_created_date', 'doaj_last_updated', 
                'title', 'pages', 'year', 'journal_title', 'journal_publisher', 
                'authors', 'subjects'])

    cnx = mysql.connector.connect(user='root', password='CarM5500',
                              host='localhost', charset = 'utf8mb4', collation='utf8mb4_unicode_ci',
                              database='OA')
    
    cnx.cmd_query('drop table if exists doaj_articles;')
    
    cnx.cmd_query(query)
    cnx.commit();
    cnx.close();



create_doaj_articles_table()
journals = retrieve_journals()

for journal in journals[6955:]:
    print('Working on %s' % journal)
    journal_url = 'https://doaj.org/api/v1/search/articles/journal:"%s"' % urllib.parse.quote_plus(journal[0])
    articles = get_data( journal_url )
    write_articles_to_doaj_articles_table(articles)

