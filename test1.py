from node import Node

node = Node('192.168.0.100:3000')
node._ident = 1600
node.join('192.168.0.100:2000')
node.run()
