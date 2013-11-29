from hashlib import sha1

import zerorpc


RING_SIZE = 160


def remote_call(address, method, args):
    client = zerorpc.Client()
    client.connect('tcp://%s' % address)
    res = client(method, *args)
    client.close()
    return res


def circular_range(i, p1, p2):
    if p1 < p2:
        return  i > p1 and i < p2
    else:
        return (i > p1 and i > p2) or (i < p1 and i < p2)


class Node(object):
    def __init__(self, address):
        self.finger_table = []
        self.keys = {}
        self.address = address
        self._ident = None
        self.successor = None
        self.predecessor = None

    def hash_key(self, key):
        return int(sha1(key).hexdigest(), 16)

    def hash_node(self, address):
        return int(sha1(address).hexdigest(), 16)

    @property
    def ident(self):
        if self._ident:
            return self._ident

        else:
            self._ident = self.hash_node(self.address)
            return self._ident

    def dict(self):
        if self.successor:
            self.successor['ident'] = str(self.successor['ident'])

        return {'address': self.address, 'ident': str(self.ident),
                'successor': self.successor, 'predecessor': self.predecessor}

    def join(self, successor_addr=None):
        if successor_addr:
            node = remote_call(successor_addr, 'find_successor',
                [str(self.ident)])
            self.successor = {
                'address': node['address'],
                'ident': node['ident']
            }

        for i in range(RING_SIZE):
            self.finger_table.append({
                'start': int((self.ident + (pow(2, i - 1))) % pow(2, RING_SIZE))
            })

        self.create_finger_table()

    def leave(self):
        self.update_others(self.successor)
        remote_call(self.successor['address'], 'update_predecessor', [self.predecessor])

        # transfer the keys
        for key in self.keys:
            remote_call(self.predecessor['address'], 'add_key', [key, self.keys[key]])

    def create_finger_table(self):
        if self.successor:
            node = remote_call(self.successor['address'],
                'find_successor', [str(self.finger_table[0]['start'])])

            self.successor = node
            self.finger_table[0]['node'] = node
            node = remote_call(self.successor['address'], 'dict', [])
            self.predecessor = node['predecessor']

            remote_call(node['address'], 'update_predecessor', [self.dict()])

            for i in range(RING_SIZE - 1):
                start = self.finger_table[i + 1]['start']

                if circular_range(start, self.ident,
                    int(self.finger_table[i]['node']['ident'])):
                    self.finger_table[i + 1]['node'] = self.finger_table[i]['node']

                else:
                    self.finger_table[i + 1]['node'] = remote_call(
                        self.successor['address'], 'find_successor',
                        [str(self.finger_table[i + 1]['start'])])

            self.update_others(self.dict())

            # Request the successor for keys which belong to this node
            keys = remote_call(self.successor['address'],
                'pop_preceding_keys', [str(self.ident)])

            for key in keys:
                self.keys[key] = keys[key]

        else:
            for i in range(RING_SIZE):
                self.finger_table[i]['node'] = self.dict()
            successor = {
                'address': self.address,
                'ident': self.ident
            }
            self.successor = self.predecessor = successor

    def update_others(self, node):
        for i in range(RING_SIZE):
            ind = int((self.ident - (pow(2, i - 1))) % pow(2, RING_SIZE))

            p = self.find_predecessor(ind)
            #print p['address']
            if int(p['ident']) != self.ident:
                remote_call(p['address'], 'update_finger_table', [node, str(i)])

    def update_finger_table(self, node, finger_id):
        finger_id = int(finger_id)

        if self.ident == int(node['ident']):
            self.finger_table[finger_id]['node'] = node

            if finger_id == 0:
                self.successor = {
                    'address': node['address'],
                    'ident': node['ident']
                }

        elif circular_range(int(node['ident']), int(self.ident),
            int(self.finger_table[finger_id]['node']['ident'])):
            self.finger_table[finger_id]['node'] = node
            #remote_call(self.predecessor['address'], 'update_finger_table',
            #    [node, finger_id])

            if finger_id == 0:
                self.successor = {
                    'address': node['address'],
                    'ident': node['ident']
                }

    def update_predecessor(self, node):
        self.predecessor = {
            'address': node['address'],
            'ident': node['ident']
        }
        return 0

    def find_successor(self, ident):
        ident = int(ident)

        pred = self.find_predecessor(ident)
        pred['ident'] = int(pred['ident'])
        pred['successor']['ident'] = str(pred['successor']['ident'])

        if pred['successor']['ident'] == self.ident:
            return self.dict()
        else:
            return pred['successor']

    def find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = self.successor

        while not circular_range(int(ident), int(node_id), int(successor['ident'])):
            if int(node_id) == self.ident:
                node = self.closest_preceding_finger(ident)

                if int(node['ident']) == int(self.ident):
                    continue
            else:
                node = remote_call(node['address'],
                    'closest_preceding_finger', [str(ident)])

            node_id = node['ident']
            successor = node['successor']

        return node

    def closest_preceding_finger(self, ident):
        i = RING_SIZE - 1

        while i >= 0:
            node = self.finger_table[i]['node']
            if circular_range(int(node['ident']), int(self.ident), int(ident)):
                node = remote_call(node['address'], 'dict', [])
                return node
            i -= 1

        node = self.dict()
        return node

    def run(self):
        rpc = zerorpc.Server(self)
        print self.address
        rpc.bind('tcp://%s' % self.address)
        rpc.run()

    def pop_preceding_keys(self, ident):
        return_keys = {}

        for key in self.keys.keys():
            if self.hash_key(key) <= int(ident):
                return_keys.update({key: self.keys.pop(key)})
        return return_keys

    def get_key(self, key):
        key_ident = self.hash_key(key)
        node = self.find_successor(key_ident)

        if int(node['ident']) == int(self.ident):
            addr = self.address

            if key in self.keys:
                status = self.keys[key]
            else:
                status = 'Not found'

            return {'node': addr, 'status': status}
        else:
            return remote_call(node['address'], 'get_key', [key])

    def add_key(self, key, val):
        key_ident = self.hash_key(key)
        node = self.find_successor(key_ident)

        if int(node['ident']) == self.ident:
            self.keys[key] = val
            addr = self.address
            return {'node': addr, 'status': 'Added'}
        else:
            print 'Sending to remote %s' % node['address']
            return remote_call(node['address'], 'add_key', [key, val])

    def delete_key(self, key):
        key_ident = self.hash_key(key)
        node = self.find_successor(key_ident)

        if int(node['ident']) == self.ident:
            addr = self.address

            if key in self.keys:
                self.keys.pop(key)
                status = 'Deleted'
            else:
                status = 'Not found'

            return {'node': addr, 'status': status}

        else:
            print 'Sending to remote %s' % node['address']
            return remote_call(node['address'], 'delete_key', [key])
