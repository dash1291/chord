chorddht
========

A little WIP implementation of Distributed Hash Table based on Chord_

.. _Chord: http://en.wikipedia.org/wiki/Chord_(peer-to-peer)

Installation
------------

``pip install -e git+https://github.com/dash1291/dht#egg=chorddht``

Usage
-----

chordnode
~~~~~~~~~

After installation, a node can be launched with the following command:

``chordnode 192.168.0.100:2000``

This will launch a node bound to address ``192.168.0.100``, and port ``2000``. This will also be the first node in the network.

More nodes can be added to the network with the following command:

``chordnode 192.168.0.100:3000 192.168.0.100:2000``

The second argument ``192.168.0.100:2000`` points to one of the existing nodes in the network.


ChordConnection
~~~~~~~~~~~~~~~

ChordConnection is the client that can be used to add or fetch data from the network.

Example:

.. code-block:: pycon
  >>> from chorddht.client import ChordConnection
  >>> conn = ChordConnection('192.168.0.100:2000')
  >>> conn.set('test_key', 'test_value')
  >>> True
  >>> conn.get('test_key')
  >>> 'test_value'

One can pass the address of any of the existing nodes in the network.
