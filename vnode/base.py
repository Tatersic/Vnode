import asyncio
from functools import wraps
from collections import OrderedDict
from typing import Any, Callable, Coroutine, Dict, Iterable, List, NoReturn, Optional, Type, Union, overload
from typing import OrderedDict as T_OrderedDict

from exceptions import *

class VnodeObject:
    ...

class BaseMessage(VnodeObject):
    
    __slots__ = ["owner", "data"]

    def __init__(self, owner: "BaseNode", *, data: Any = None) -> None:
        self.owner= owner
        self.data = data
    
    def send(self, receiver: str, *, net: Optional["BaseNetwork"] = None) -> None:
        net = net or self.owner.network
        if not net:
            raise VnodeError("Failed to link with network.")
        net.send(receiver, self)
    
    def __str__(self) -> str:
        return f"{self.__class__.__qualname__}({self.data},from={self.owner})"

class InputMessage(BaseMessage):
    """
    Input by users.
    """

    __slots__ = ["args", "kwds"]

    def __init__(self, *args, **kwds) -> None:
        self.args = args
        self.kwds = kwds

    def bind(self, func: Callable) -> Any:
        return func(*self.args, **self.kwds)
    
    def __str__(self) -> str:
        return f"{self.__class__.__qualname__}({self.args},{self.kwds})"

class BaseEvent(BaseMessage):
    ...

class NetwortInitedEvent(BaseEvent):

    __slots__ = ["network"]

    def __init__(self, net: "BaseNetwork") -> None:
        self.network = net
    
    def __str__(self) -> str:
        return f"{self.__class__.__qualname__}({self.network})"

class BaseNode(VnodeObject):

    __slots__ = ["name", "connections", "network"]

    def __init__(
        self,
        name: str,
        connections: List[str] = [],
        *,
        network: Optional["BaseNetwork"] = None
    ) -> None:
        self.name = name
        self.connections = connections
        self.network = network
    
    async def __respond__(self, msg: BaseMessage) -> None:
        print(f"require recieve: {msg}")
        for rev in self.connections:
            msg.send(rev)
    
    def respond(self, msg: BaseMessage) -> None:
        if not self.network:
            raise VnodeError("Failed to link with network.")
        self.network.task(self.__respond__(msg))
    
    def link(self, net: "BaseNetwork") -> None:
        self.network = net
    
    def copy(self, connections: List[str], network: "BaseNetwork") -> "BaseNode":
        return BaseNode(self.name, connections, network=network)
    
    def __call__(self, *args: Any, **kwds: Any) -> None:
        msg = InputMessage(*args, **kwds)
        self.respond(msg)
        return 

    def __str__(self) -> str:
        return f"<node {self.name} in {self.network}>"
    
    __repr__ = __str__

class BaseNetwork(VnodeObject):

    __slots__ = ["nodes", "loop", "activated"]

    nodes: T_OrderedDict[str, BaseNode]

    def __init__(
        self,
        *nodes: BaseNode,
        loop: Optional[asyncio.BaseEventLoop] = None
    ) -> None:
        self.nodes = OrderedDict()
        for n in nodes:
            n.link(self)
            self.nodes[n.name] = n
        
        self.activated = False

        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.new_event_loop()
    
    def _init(self, first_task: Optional[Coroutine] = None) -> None:
        self.activated = True
        for n in self.nodes.values():
            self.task(n.__respond__(NetwortInitedEvent(self)))
        if first_task:
            self.task(first_task)
        self.loop.run_forever()
    
    def add(self, *nodes: BaseNode) -> None:
        for n in nodes:
            self[n.name] = n

    def task(self, task: Coroutine) -> Optional[asyncio.Future]:
        if not self.activated:
            self._init(task)
        else:
            t = asyncio.ensure_future(task, loop=self.loop)
            return t
    
    def send(self, receiver: str, message: BaseMessage) -> None:
        self[receiver].respond(message)

    def __getitem__(self, key: Union[int, str]) -> BaseNode:
        if isinstance(key, str):
            return self.nodes[key]
        else:
            return list(self.nodes.values())[key]
    
    def __setitem__(self, key: str, node: BaseNode) -> None:
        self.nodes[key] = node

    def __iter__(self) -> Iterable[BaseNode]:
        return self.nodes.values()
    
    def __contains__(self, node: Union[str, BaseNode]) -> bool:
        if isinstance(node, str):
            return node in self.nodes
        else:
            return node in self.nodes.values()

if __name__ == "__main__":
    node = BaseNode("HelloWorld")
    net = BaseNetwork(node)
    v = net["HelloWorld"]
    v("HelloWorld")