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


def get_consumer_cheques(discount_card_id=_discount_card_id):
    conn = connect(host='node1.allende.bigkore.com', port=21050)
    cursor = conn.cursor()
    query = "select consumer_id, date1, good_name, quantity, cost_without_discount, cost_with_discount, shop_id from test.consumer_date_purchases where discount_card_id='%s'" % (discount_card_id)
    cursor.execute(query)
    results = cursor.fetchall()
    results = [[str(r) for r in row] for row in results]
    conn.close()

    def links_from_row(row):
        return [
            Link('discount_card_id=' + row[0], 'date=' + row[1]),
            Link('date=' + row[1], 'good_name=' + row[2]),
            Link('good_name=' + row[2], 'quantity=' + row[3]),
            Link('good_name=' + row[2], 'cost=' + row[4]),
            Link('good_name=' + row[2], 'cost_discounted=' + row[5]),
            Link('good_name=' + row[2], 'shop_id=' + row[6])
        ]
    links = []
    for row in results:
        links += links_from_row(row)

    tree = hierarchy(links, 'discount_card_id=' + discount_card_id)
    return json.dumps(_to_json(tree))


class Link(object):
    def __init__(self, parent_name, name):
        self.id = name, self.parent_id = parent_name, self.name = name


class Node(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.children = []


def _find_parent_node(tree, group):
    if tree.id == group.parent_id:
        return tree
    for child in tree.children:
        found = _find_parent_node(child, group)
        if found:
            return found
    return None


def hierarchy(links, root_name):
    tree = Node(root_name, root_name)
    while links:
        to_remove = []
        for g in links:
            parent_node = _find_parent_node(tree, g)
            if not parent_node:
                continue
            node = Node(g.id, g.name)
            parent_node.children.append(node)
            to_remove.append(g)
        for g in to_remove:
            links.remove(g)
    return tree


def _to_json(node):
    node_json = {
        'name': node.name,
        'children': [_to_json(child_node) for child_node in node.children]
    }
    return node_json


# print (get_consumer_basket())
