from node import Node

node = Node('192.168.0.100:2000')
node._ident = 1
node.join()
node.run()
