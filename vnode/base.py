import asyncio
from functools import wraps
from collections import OrderedDict
from typing import Any, Callable, Coroutine, Dict, Iterable, List, NoReturn, Optional, Tuple, Type, Union, overload
from typing import OrderedDict as T_OrderedDict

from log import *
from exceptions import *

class VnodeObject:
    __slots__ = []

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
    
    async def aio_send(self, receiver: str, *, net: Optional["BaseNetwork"] = None) -> None:
        net = net or self.owner.network
        if not net:
            raise VnodeError("Failed to link with network.")
        await net[receiver].__respond__(self)
    
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
    
    async def aio_bind(self, func: Callable) -> Any:
        return await func(*self.args, **self.kwds)
    
    def __str__(self) -> str:
        return f"{self.__class__.__qualname__}({self.args},{self.kwds})"

class NetwortInitedMessage(BaseMessage):

    __slots__ = ["network", "lock", "uninit"]

    def __init__(
        self,
        net: "BaseNetwork", 
        lock: asyncio.Lock, 
        uninit: List[str]
    ) -> None:
        self.network = net
        self.lock = lock
        self.uninit = uninit
    
    def __str__(self) -> str:
        return f"{self.__class__.__qualname__}({self.network})"

class _OnMessage:
    
    __slots__ = ["func", "message"]

    def __init__(self, func: Callable[["BaseNode", BaseMessage], Any], message: Type[BaseMessage]) -> None:
        if not asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(node: BaseNode, msg: BaseMessage):
                return func(node, msg)
            self.func = wrapper
        else:
            self.func = func
        self.message = message

def on_message(msg: Type[BaseMessage]) -> Callable[[Callable], _OnMessage]:
    def wrapper(func: Callable[["BaseNode", BaseMessage], Any]) -> _OnMessage:
        return _OnMessage(func, msg)
    return wrapper

async def _wait_lock(func: Coroutine, lock: asyncio.Lock) -> Any:
    await lock.acquire()
    await func

class NodeMeta(type):

    __message__: Dict[Tuple[Type[BaseMessage]], Callable[[BaseMessage], Any]]

    def __new__(cls, name: str, bases: tuple, attrs: Dict[str, Any]):
        if "__message__" in attrs:
            raise VnodeError("Invalid attribute '__message__'")
        n_attrs = {"__message__":{}}
        if name != "BaseNode":
            for node in bases:
                n_attrs["__message__"].update(node.__message__)
        for n, m in attrs.items():
            if isinstance(m, _OnMessage):
                n_attrs["__message__"][(m.message,)] = m.func
            else:
                n_attrs[n] = m
        return type.__new__(cls, name, bases, n_attrs)

    def _get_message_handler(self, msg: BaseMessage) -> Optional[Callable]:
        return self.__message__.get((type(msg),), None)

class BaseNode(VnodeObject, metaclass=NodeMeta):

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
        logger.info(f"require recieve: {msg}")
        
    def respond(self, msg: BaseMessage) -> None:
        if not self.network:
            raise VnodeError("Failed to link with network.")
        self.network.task(self.__respond__(msg))
    
    def join(self, net: "BaseNetwork") -> None:
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
            n.join(self)
            self.nodes[n.name] = n
        
        self.activated = False

        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.new_event_loop()
    
    def _init(self, first_task: Optional[Coroutine] = None) -> None:
        self.activated = True
        lock = asyncio.Lock()
        l = list(self.nodes.keys())
        for n in self.nodes.values():
            self.task(n.__respond__(NetwortInitedMessage(self, lock, l)))
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
            if key not in self.nodes:
                raise VnodeError(f"node {key} is not in network {self}.")
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
    logger.setLevel(INFO)
    node = BaseNode("HelloWorld")
    net = BaseNetwork(node)
    v = net["HelloWorld"]
    v("HelloWorld")