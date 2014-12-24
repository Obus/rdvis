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
    print query
    cursor.execute(query)
    column_names = [c[0] for c in cursor.description]
    column_types = [get_internal_type_from_mssql_type(c[1]) for c in cursor.description]
    results = cursor.fetchall()

    def trim_if_string(i, v):
        if column_types[i] == 'string' and v:
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


def get_consumer_cheques(discount_card_id=_discount_card_id):
    conn = connect(host='node1.allende.bigkore.com', port=21050)
    cursor = conn.cursor()
    query = "select consumer_id, date1, good_name, quantity, cost_without_discount, cost_with_discount, shop_id from test.consumer_date_purchases where consumer_id='%s'" % (discount_card_id)
    print query
    cursor.execute(query)
    results = cursor.fetchall()
    results = [[str(r) for r in row] for row in results]
    conn.close()

    root_Node = id_name_Node('d=' + discount_card_id)
    date_node_dict = {}

    for row in results:
        date = row[1]
        date_node = None
        if date in date_node_dict:
            date_node = date_node_dict[date]
        else:
            date_node = id_name_Node('date=' + date)
            date_node_dict[date] = date_node
        good_node = id_name_Node('' + row[2])
        good_node.children = [
            id_name_Node('quantity=' + row[3]),
            id_name_Node('cost=' + row[4]),
            id_name_Node('cost_discounted=' + row[5]),
            id_name_Node('shop_id=' + row[6])
        ]
        date_node.children.append(good_node)

    root_Node.children += sorted(date_node_dict.values(), cmp=lambda x, y: - cmp(x.name, y.name))
    return json.dumps(_to_json(root_Node))


def get_consumer_group_cheques(discount_card_id=_discount_card_id):
    conn = connect(host='node1.allende.bigkore.com', port=21050)
    cursor = conn.cursor()
    query = "select consumer_id, date1, group23, quantity from test.consumer_date_purchases_group23 where consumer_id='%s'" % (discount_card_id)
    print query
    cursor.execute(query)
    results = cursor.fetchall()
    results = [[str(r) for r in row] for row in results]
    conn.close()

    root_Node = id_name_Node('d=' + discount_card_id)
    date_node_dict = {}

    for row in results:
        date = row[1]
        date_node = None
        if date in date_node_dict:
            date_node = date_node_dict[date]
        else:
            date_node = id_name_Node('date=' + date)
            date_node_dict[date] = date_node
        good_node = id_name_Node('' + row[2])
        good_node.children = [
            id_name_Node('quantity=' + row[3]),
        ]
        date_node.children.append(good_node)

    root_Node.children += sorted(date_node_dict.values(), cmp=lambda x, y: - cmp(x.name, y.name))
    return json.dumps(_to_json(root_Node))


def id_name_Node(name):
    return Node(name, name)


class Node(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.children = []



def _to_json(node):
    node_json = {
        'name': node.name,
        'children': [_to_json(child_node) for child_node in node.children]
    }
    return node_json


# print (get_consumer_basket())
