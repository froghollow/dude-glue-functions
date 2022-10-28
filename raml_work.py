import os
import sys
import json

import urllib

sys.path.insert( 0, os.path.abspath(os.getcwd() + '/common/glue_functions/python/') )

import glue_functions as glu

help(glu)


def url_to_sql( url_input ):

    from urllib.parse import urlparse
    parts = urlparse(url_input)

    parsed_url = {
        'UrlInput' : url_input,
        'BaseUrl' : f"{parts.scheme}://{parts.netloc}",
        'Endpoint' : parts.path,
        'TableName' : parts.path.split('/')[-1],
        'ColumnList' : '*',
        'FilterSpec' : '',
        'SortSpec' : '',
        'PageSize' : 100,
        'PageNumber' : 1,
        'Offset' : 0,
        'Format' : 'json' # or 'csv'
    }

    #if url_input.__contains__('?'):
    if parts.query > '':
        url_parms = parts.query.replace(' ','').split('&')
        for parm in url_parms:
            parm_value = parm.split('=')[1]
            if parm.startswith('fields='):
                parsed_url['ColumnList'] = parm_value
            elif parm.startswith('filter='):
                filters = parm_value
                operators = {
                    ':lt:'  : ' < ',   # Less than
                    ':lte:' : ' <= ',  # Less than or equal to
                    ':gt:'  : ' > ',   # Greater than
                    ':gte:' : ' >= ',  # Greater than or equal to
                    ':eq:'  : ' = ',   # Equal to
                    ':in:'  : ' in '   # Contained in a given set
                    }
                '''
                if ':in:(' in filters:

                    filters[filters.index('(')+1 : filters.index(')')]

                    in_values = filters[filters.find(':in:(')+5:filters.find(')')].split(',')
                    for i, val in enumerate(in_values):in_values[i] = f"'{val}'"
                    in_values = ",".join(in_values)
                '''
                for op in operators:
                    #print ( op, operators[op] )

                    if op ==':in:':
                        lp = filters.find('(') + 1
                        rp = filters.find(')', lp)
                        in_values = filters[lp:rp]

                    filters = filters.replace(op, operators[op])
                parsed_url['FilterList'] = filters.split(',')
                #parsed_url['FilterList'] = parm_value

            elif parm.startswith('sort='):
                parsed_url['SortSpec'] = parm_value

            elif parm.startswith('format='):
                parsed_url['Format'] = parm_value

            elif parm.startswith('page[size]='):
                page_size = int(parm_value)
                parsed_url['PageSize'] = int(parm_value)

            elif parm.startswith('page[number]='):
                page_number = int(parm_value)
                parsed_url['PageNumber'] = page_number
                parsed_url['Offset'] = page_size * (page_number - 1)

            else:
                print( f'Ignoring unknown parm {parm}' )

        '''
          select {ColumnList}
            from {TableName}
           where {FilterSpec}
        order by {SortSpec}
           limit {PageSize} offset {PageSize * PageNumber} ;
        '''
        sql =  f"  select {'ColumnList'} \n    from {parsed_url['TableName']} \n"
        if parsed_url.['FilterList'] > '':
            sql += f"   where " + {' and '.join(parsed_url['FilterList'])} + " \n"
        if parsed_url.['SortSpec'] > '':
            sql += f"order by {parsed_url.['SortSpec']} \n"
        if parsed_url.['PaginationSpec'] > '':
            sql += f"  {parsed_url.['PaginationSpec']} \n"
        sql += ";"

        parsed_url['Sql'] = sql

    print (parsed_url)
    return parsed_url


#######

url_input = 'https://api.fiscaldata.treasury.gov:443/services/api/fiscal_service/v2/accounting/od/debt_to_penny?fields=record_calendar_year&sort=-record_calendar_year,-record_calendar_month'
url_input = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange?fields=country_currency_desc,exchange_rate, record_date&filter=country_currency_desc:in:('Canada-Dollar','Mexico-Peso'), record_date:gte:2020-01-01 &foobar=someshit"

url_input = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/{table_name}'


parts.query.strip('&')




parsed_url = url_to_sql(url_input)
','.join(parsed_url['ColumnList'])

'where ' + ' and '.join(parsed_url['FilterList'])


print(json.dumps(parsed_url, indent=2))


############################################################



meta = {
  "count": 3790,
  "labels": {
    "country_currency_desc": "Country - Currency Description",
    "exchange_rate": "Exchange Rate",
    "record_date": "Record Date"
  },
  "dataTypes": {
    "country_currency_desc": "STRING",
    "exchange_rate": "NUMBER",
    "record_date": "DATE"
  },
  "dataFormats": {
    "country_currency_desc": "String",
    "exchange_rate": "10.2",
    "record_date": "YYYY-MM-DD"
  },
  "total-count": 3790,
  "total-pages": 1
}


import yaml

types_json = {}

#meta['labels'].keys()
for key in meta['labels'].keys():
    print(key)
    types_json[key] = {
            'type' : meta['dataTypes'][key].lower() ,
            'displayName' : meta['labels'][key] ,
            'format' : meta['dataFormats'][key]
        }

types_json = { 'types' : types_json }

types_yaml =  yaml.dump(types_json, sort_keys=False)
print(types_yaml)

types_yaml



#############################################################################

#datadict_csv = glu.get_file("file://C:\\Users\\RMYERS07\\Downloads\\_Day Delinquent Debt Referral Compliance Report Data Dictionary.csv")
import csv
import json

    csvFilePath = "C:\\Users\\RMYERS07\\Downloads\\_Day Delinquent Debt Referral Compliance Report Data Dictionary.csv"

    # create a dictionary
    data = {}

    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:

            # Assuming a column named 'No' to
            # be the primary key
            #key = rows['No']
            key = rows['field_name']
            data[key] = rows


    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))

print(json.dumps(data , indent=2 ))
