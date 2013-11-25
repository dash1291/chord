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

    @property
    def ident(self):
        if self._ident:
            return self._ident

        else:
            self._ident = int(sha1(self.address).hexdigest(), 16)
            return self._ident

    def dict(self):
        if self.successor:
            self.successor['ident'] = str(self.successor['ident'])

        return {'address': self.address, 'ident': str(self.ident),
                'successor': self.successor, 'predecessor': self.predecessor}

    def join(self, successor_addr=None):
        if successor_addr:
            node = remote_call(successor_addr, 'find_successor', [str(self.ident)])
            self.successor = {
                'address': node['address'],
                'ident': node['ident']
            }

        for i in range(RING_SIZE):
            self.finger_table.append({
                'start': int((self.ident + (pow(2, i - 1))) % pow(2, RING_SIZE))
            })

        #print 'successor'
        #print self.successor
        self.create_finger_table()

    def create_finger_table(self):
        if self.successor:
            node = remote_call(self.successor['address'],
                'find_successor', [self.finger_table[0]['start']])

            self.successor = node
            self.finger_table[0]['node'] = node
            node = remote_call(self.successor['address'], 'dict', [])
            self.predecessor = node['predecessor']

            remote_call(node['address'], 'update_predecessor', [self.dict()])

            #print self.finger_table[0]

            for i in range(RING_SIZE - 1):
                start = self.finger_table[i + 1]['start']

                if circular_range(start, self.ident, int(self.finger_table[i]['node']['ident'])):
                    self.finger_table[i + 1]['node'] = self.finger_table[i]['node']
                    #print self.finger_table[i]['node']

                else:
                    self.finger_table[i + 1]['node'] = remote_call(
                        self.successor['address'], 'find_successor',
                        [str(self.finger_table[i + 1]['start'])])
                    #print self.finger_table[i + 1]['node']

                print self.finger_table[i]
            self.update_others()

        else:
            for i in range(RING_SIZE):
                self.finger_table[i]['node'] = self.dict()
            successor = {
                'address': self.address,
                'ident': self.ident
            }
            self.successor = self.predecessor = successor

    def update_others(self):
        print 'updating others'
        for i in range(RING_SIZE):
            ind = int((self.ident - (pow(2, i - 1))) % pow(2, RING_SIZE))

            p = self.find_predecessor(ind)
            print p['address']
            if int(p['ident']) != self.ident:
                remote_call(p['address'], 'update_finger_table', [self.dict(), str(i)])

    def update_finger_table(self, node, finger_id):
        print 'updating mine'

        finger_id = int(finger_id)

        if circular_range(int(node['ident']), int(self.ident), int(self.finger_table[finger_id]['node']['ident'])):
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
        #self.successor = pred['successor']
        if pred['successor']['ident'] == self.successor['ident']:
            return self.dict()
        else:
            return pred['successor']

    def find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = self.successor

        while not circular_range(int(ident), int(node_id), int(successor['ident']) or int(ident)):
            if int(node_id) == self.ident:
                node = self.closest_preceding_finger(node_id)

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
        rpc.bind('tcp://%s' % self.address)
        rpc.run()


    """
    def get_key(id):


    def add_key(key, val):


    def delete_key(key):


    def change_key(key, val):
    """
