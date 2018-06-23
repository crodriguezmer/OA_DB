import pandas as pd
import numpy as np
import mysql.connector
from itertools import compress


data = pd.read_csv('https://doaj.org/csv', parse_dates= ['Added on Date'], 
                   infer_datetime_format= True)
data.loc[:,'journal_id'] = data.loc[:,'Added on Date'].rank(method='first').astype(int)+1


columns_varchar = {
    'name':'Journal title',
    'url':'Journal URL',
    'alt_name':'Alternative title',
    'issn':'Journal ISSN (print version)',
    'eissn':'Journal EISSN (online version)',
    'publisher':'Publisher',
    'institution':'Society or institution',
    'platform':'Platform, host or aggregator',
    'country':'Country of publisher',
    'processing_charges_url':'APC information URL',
    'processing_charges_currency':'Currency',
    'submission_fee_url':'Submission fee URL',
    'submission_fee_currency':'Submission fee currency',
    'prior_year_articles_url':'Number of articles information URL',
    'waiver_policy_url':'Waiver policy information URL',
    'archiving_policy_digital':'Digital archiving policy or program(s)',
    'archiving_policy_national_library':'Archiving: national library',
    'archiving_policy_other':'Archiving: other',
    'archiving_policy_url':'Archiving infomation URL',
    'permanent_article_identifiers':'Permanent article identifiers',
    'download_statistics_url':'Download statistics information URL',
    'full_text_formats':'Full text formats',
    'keywords':'Keywords',
    'language':'Full text language',
    'editorial_board_url':'URL for the Editorial Board page',
    'review_process':'Review process',
    'review_process_url':'Review process information URL',
    'scope_url':"URL for journal's aims & scope",
    'author_instruction_url':"URL for journal's instructions for authors",
    'plagiarism_policy_url':'Plagiarism information URL',
    'open_access_url':"URL for journal's Open Access statement",
    'embedded_licensing_info_url':'URL to an example page with embedded licensing information',
    'license':'Journal license',
    'license_attributes':'License attributes',
    'license_url':'URL for license terms',
    'deposit_policy_directory':'Deposit policy directory',
    'unrestricted_author_copyright':'Author holds copyright without restrictions',
    'copyright_info_url':'Copyright information URL',
    'unrestricted_author_publishing':'Author holds publishing rights without restrictions',
    'author_publishing_url':'Publishing rights information URL',
    'subjects':'Subjects'
}

columns_float = {'processing_charges_amount':'APC amount',
                 'submission_fee_amount':'Submission fee amount',
                 'prior_year_articles':'Number of articles publish in the last calendar year',
                 'avg_weeks_to_publish':'Average number of weeks between submission and publication'
}
columns_int = {'journal_id':'journal_id',
               'first_year_open_access':'First calendar year journal provided online Open Access content'                
}
columns_bool = {'processing_charges':'Journal article processing charges (APCs)',
                'submission_fee':'Journal article submission fee',
                'waiver_policy':'Journal waiver policy (for developing country authors etc)',
                'full_text_crawl_permit':'Journal full-text crawl permission',
                'download_statistics':'Journal provides download statistics',
                'plagiarism_policy':'Journal plagiarism screening policy',
                'embedded_licensing_info':'Machine-readable CC licensing information embedded or displayed in articles',
                'boai_unrestricted_reuse':'Does this journal allow unrestricted reuse in compliance with BOAI?',
                'doaj_seal':'DOAJ Seal',
                'accepted_after_2014':'Tick: Accepted after March 2014'
}
columns_date = {'added_date':'Added on Date'}

all_columns = {**columns_int, **columns_varchar, **columns_float, **columns_bool, **columns_date}
all_columns_inv = {v: k for k, v in all_columns.items()}
data.rename(columns= all_columns_inv, inplace=True)
# get non-nan values
isnotnan = ~data.isnull() # booleans of non-nans


query = '''
create table doaj_journals
(
%s int,
%s int,

%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(500) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,
%s text CHARACTER SET utf8,

%s float,
%s float,
%s float,
%s float,

%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,
%s varchar(300) CHARACTER SET utf8,

%s timestamp
);
''' % tuple(all_columns.keys())

cnx = mysql.connector.connect(user='root', password='',
                              host='localhost',
                              database='OA')

cnx.cmd_query('drop table if exists doaj_journals;')

cnx.cmd_query(query)
cnx.commit()
cnx.close()



cnx = mysql.connector.connect(user='root', password='',
                              host='localhost',
                              database='OA')
for i in data.index:
    
    column_names       = isnotnan.iloc[i, :].index
    column_non_missing = isnotnan.iloc[i, :].values

    non_missing_columns = list(compress(column_names, column_non_missing))
    non_missing_data    = data.loc[i, non_missing_columns]

    # concatenate into single string all the column names
    col_name_tuple = "("
    for column in non_missing_data.index:
        col_name_tuple += column + ', '
    col_name_tuple = col_name_tuple[:-2] + ')'
    
    query = '''
    insert into doaj_journals %s
    values %s
    ''' % ( col_name_tuple, str(tuple(non_missing_data.values)) )
        
    cnx.cmd_query(query)
    cnx.commit()

cnx.close()

