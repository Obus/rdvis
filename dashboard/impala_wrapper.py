__author__ = 'senov'

from impala.dbapi import connect
import json


_discount_card_id = '2775087827816'
_table_name = 'consumer_top_goods'
_mssql2int_type_dict = {
    'STRING_TYPE': 'string',
    'BIGINT_TYPE': 'numeric',
    'DOUBLE_TYPE': 'numeric'
}


def get_internal_type_from_mssql_type(mssql_type):
    return _mssql2int_type_dict[mssql_type]


def get_consumer_basket(discount_card_id=_discount_card_id, table_name=_table_name, query=None):
    conn = connect(host='node1.allende.bigkore.com', port=21050)
    cursor = conn.cursor()
    if not query:
        query = "select * from test.%s where discount_card_id='%s'" % (table_name, discount_card_id)
    cursor.execute(query)
    column_names = [c[0] for c in cursor.description]
    column_types = [get_internal_type_from_mssql_type(c[1]) for c in cursor.description]
    results = cursor.fetchall()

    def trim_if_string(i, v):
        if column_types[i] == 'string':
            return v.strip()
        return v

    results = [[trim_if_string(i, v) for i, v in enumerate(row)] for row in results]

    conn.close()

    json_result = {
        'col_names': column_names,
        'col_types': column_types,
        'data': results
    }

    return json.dumps(json_result)


# print (get_consumer_basket())
