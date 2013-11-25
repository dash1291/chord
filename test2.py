from node import Node

node = Node('192.168.0.100:4000')
node._ident = 1000
node.join('192.168.0.100:3000')
node.run()
