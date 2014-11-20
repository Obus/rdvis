__author__ = 'senov'
import pymssql
import json


_TABLE = "temp_goods_groups"
_USER = "SVC_bigkore2BI01"
_PASS = "ifm2K)tPR8"
_SERVER_PORT = "172.27.192.148:1433"
_DATABASE = "Land_Inbound"



class FlatNode(object):
    def __init__(self, id, parent_id, name):
        self.id = id
        self.parent_id = parent_id
        self.name = name


def fix_s(s):
    return s[1:-1]

def get_flat_tree(conn, table_name):
    flat_nodes = []
    cursor = conn.cursor()
    cursor.execute("select id, parent_id, name from %s" % table_name)
    row = cursor.fetchone()
    while row:
        flat_nodes.append(FlatNode(fix_s(row[0]), fix_s(row[1]), fix_s(row[2])))
        row = cursor.fetchone()
    conn.close()
    return flat_nodes


class Node(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.children = []


def find_parent_node(tree, group):
    if tree.id == group.parent_id:
        return tree
    for child in tree.children:
        found = find_parent_node(child, group)
        if found:
            return found
    return None


_name = "name"
_children = "children"
def to_json(node):
    node_json = {
        _name: node.name,
        _children: [to_json(child_node) for child_node in node.children]
    }
    return node_json


def get_tree(table=_TABLE, database=_DATABASE, server=_SERVER_PORT, user=_USER, password=_PASS):
    conn = pymssql.connect(server, user, password, database)
    flat_nodes = get_flat_tree(conn, table)

    parents = set(r.parent_id for r in flat_nodes)
    children = set(r.id for r in flat_nodes)
    non_children = parents - children
    if len(non_children) != 1:
        raise BaseException("Exactly on non-children id expected (root), but got: " + ",".join(non_children))
    root_id = next(iter(non_children))

    tree = Node(root_id, root_id)

    while flat_nodes:
        to_remove = []
        for g in flat_nodes:
            parent_node = find_parent_node(tree, g)
            if not parent_node:
                continue
            node = Node(g.id, g.name)
            parent_node.children.append(node)
            to_remove.append(g)
        for g in to_remove:
            flat_nodes.remove(g)

    return to_json(tree)


print(json.dumps(get_tree()))