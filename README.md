# Vnode
A node network support for Python.

## Quick start
```py
from vnode import *

@network
class Net:

    @node(requests={"__start__": 0})
    def node0():
        return 5
    
    @node(connections={"__return__": 0}, requests={"node0": "num"})
    def node1(num):
        return num + 2

print(Net())
# output: 7
```