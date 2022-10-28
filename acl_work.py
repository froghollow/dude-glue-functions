import os
import sys

sys.path.insert( 0, os.path.abspath(os.getcwd() + '/common/glue_functions/python/') )

import glue_functions as glu

help(glu)


glue_database_name = 'dtl-dev-fsdatalake'
glue_tabname_regex = 'tstg'
tables, tablenames = glu.list_glue_tables(glue_database_name, glue_tabname_regex)

tables[-2]

import json
acl_file = 's3://wc2h-dtl-dev-code/config/dudeAclColumns.json'
acl_columns = json.loads(glu.get_file(acl_file))
def get_table_acl_columns(tablename):
    ''' Returns a JSON list of the ACL columns (if any) for tablename from the acl_config_file '''
    for table in acl_columns:
        if table == tablename:
            return acl_columns[table]
    return []

for table in tables:
    tablename = table['Name'].replace('fsdatalake_dw_', '')
    print (tablename)

    cols = get_table_acl_columns(tablename)
    for col in cols:
        print('', col)
    break

tablename
acl_columns[tablename]
table['Name']
dicts = table['StorageDescriptor']['Columns']

colname  = 'uuid_pk'
dicts[index]

for acl_col in acl_columns[tablename]:
    acl_colname = acl_col['ColName']
    index = next((i for i, item in enumerate(dicts) if item["Name"] ==  acl_colname), None)
    key = 'DeIdentMethod'
    val = acl_col['DeIdentMethod']
    table['StorageDescriptor']['Columns'][index]['Parameters'] = { key : val }

glu.crup_glue_table( glue_database_name, table['Name'], table['StorageDescriptor']['Columns'] )



#def crup_glue_table( glue_database_name, glue_table_name, glue_sd_columns ):
def crup_glue_table( glue_database_name, glue_table_name, glue_table_input_template ):
    ''' CReate or UPdate Glue Table '''

    glue_db = glue.get_database( Name = glue_database_name )



    # convert to Glue-compatible data types
    tds_to_glue = {
        'big_int'   : 'bigint',
        'bytes'     : 'byte',
        'char'      : 'string',
        'date'      : 'timestamp',
        'smallint'  : 'short',
        'text'      : 'string',
    }
    for sd_col in glue_sd_columns:
        if  sd_col['Type'] in tds_to_glue.keys():
            sd_col['Type'] =  tds_to_glue[sd_col['Type']]

    glue_table_input['TableInput']['Name'] = glue_table_name
    glue_table_input['TableInput']['StorageDescriptor']['Columns'] = glue_sd_columns
    if 'LocationUri' in glue_db['Database'].keys():
        glue_table_input['TableInput']['StorageDescriptor']['Location'] = glue_db['Database']['LocationUri'] + '{}/'.format(glue_table_name.upper())

    try:
        response = glue.create_table (
            DatabaseName = glue_database_name,
            TableInput = glue_table_input['TableInput']
        )
        status = 'created'
    except glue.exceptions.AlreadyExistsException:
            response = glue.update_table (
                DatabaseName = glue_database_name,
                TableInput = glue_table_input['TableInput']
        )
        status = 'updated'

    print(f"Glue Table {glue_table_name} {status} in Database {glue_database_name}" )

    return response



import boto3
glue = boto3.client('glue')
glue_dbs = glue.get_databases()
