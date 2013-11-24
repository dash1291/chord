from hashlib import sha1

import zerorpc


RING_SIZE = 4


def remote_call(address, method, args):
    client = zerorpc.Client()
    client.connect(address)
    res = client(method, *args)
    return res


class Node(object):
    def __init__(self, address, successor=None):
        self.finger_table = []
        self.keys = {}
        self.address = address
        self.successor = successor
        self._ident = None

    @property
    def ident(self):
        if self._ident:
            return self._ident
        else:
            self._ident = int(sha1(self.address).hexdigest(), 16)
            return self._ident

    def dict(self):
        return {'address': self.address, 'ident': self.ident,
                'successor': self.successor}

    def create_finger_table():
        return 0

    def find_successor(self, ident):
        pred = self.find_predecessor(ident)
        return pred['successor']

    def find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = node['successor']

        while not(ident > node_id and ident <= successor['ident']):

            if node_id == self.ident:
                node = node.closest_preceding_finger(node_id)
            else:
                node = remote_call(node['address'],
                    'closest_preceding_finger', [ident])

            node_id = node['ident']

        return node

    def closest_preceding_finger(self, ident):
        i = RING_SIZE
        while i > 1:
            node = self.finger_table[i]['node']
            if node['ident'] < ident and node['ident'] > self.ident:
                return node
            i -= 1

        return self.dict()


    """
    def get_key(id):


    def add_key(key, val):


    def delete_key(key):


    def change_key(key, val):
    """


rpc = zerorpc.Server(Node())
rpc.bind('0.0.0.0:4000')
rpc.run()
