from node import Node

node = Node('192.168.0.100:5000')
node._ident = 7000000
node.join('192.168.0.100:4000')
node.run()
