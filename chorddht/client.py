import zerorpc


class ChordConnection(object):
    def __init__(self, node_address):
        self.node_address = node_address

    def _remote_call(self, method, args):
        client = zerorpc.Client()
        client.connect('tcp://%s' % self.node_address)
        res = client(method, *args)
        client.close()
        return res

    def get(self, key):
        res = self._remote_call('get_key', [key])

        if res['status'] == 'Not found':
            return None
        else:
            return res['status']

    def set(self, key, val):
        res = self._remote_call('add_key', [key, val])
        return res['status'] == 'Added'

    def remove(self, key):
        res = self._remote_call('delete_key', [key])

        if res['status'] == 'Not found':
            raise KeyError
