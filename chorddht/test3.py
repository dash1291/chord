from node import Node

class TestNode(Node):
	def hash_key(self, key):
		return int(key)

node = TestNode('192.168.0.100:5000')
node._ident = 7000000
node.join('192.168.0.100:4000')
node.run()
