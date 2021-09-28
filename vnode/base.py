import asyncio
from functools import wraps
from inspect import signature
from collections import OrderedDict, defaultdict, deque
from asyncio.coroutines import iscoroutinefunction
from typing import (Any, Callable, Coroutine, Dict, Iterable, List, NoReturn, 
    Optional, Tuple, Deque, Type, TypeVar, Union, overload)
from typing import OrderedDict as T_OrderedDict

from log import *
from exceptions import *

class VnodeObject:
    __slots__ = []

"""
========================================================

Message Part

========================================================
"""

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

class BaseEvent(BaseMessage):

    __slots__ = []

    @property
    def name(self) -> str:
        return self.__class__.__name__

class Port(VnodeObject):

    __slots__ = ["id", "name"]

    id: Optional[int]
    name: Optional[str]

    def __init__(self, id_or_name: Union[int, str, "Port"], *, node: Optional["BaseNode"] = None) -> None:
        if isinstance(id_or_name, str):
            self.name = id_or_name
            self.id = None
        elif isinstance(id_or_name, int):
            self.id = id_or_name
            self.name = None
        else:
            self.name = id_or_name.name
            self.id = id_or_name.id
        if node:
            self.complement(node)
        super().__init__()
    
    def __eq__(self, other: "T_Port") -> bool:
        oth = Port(other)
        if self.id != None and oth.id != None:
            return self.id == oth.id
        elif self.name != None and oth.name != None:
            return self.name == oth.name
        else:
            return False
    
    def complement(self, node) -> None:
        ports: list = node.ports
        try:
            if (not self.name) and self.id != None:
                self.name = ports[self.id]
            else:
                self.id = ports.index(self.name)
        except (ValueError, IndexError):
            raise VnodeError(f"{self} cannot match {node.ports}")
    
    def __str__(self) -> str:
        if self.name == None:
            return f"<node port {self.id}>"
        else:
            return f"<node port {self.name}>"

T_Port = TypeVar("T_Port", Port, int, str)

class ConnectionRequestMessage(BaseMessage):
    """
    When this message is sent to node A from node B,
    node A should conect to node B.
    """
    
    __slots__ = ["port"]
    
    def __init__(self, owner: "BaseNode", port: T_Port, *, data: Any = None) -> None:
        if isinstance(port, Port):
            self.port = port
        else:
            self.port = Port(port, node=owner)
        self.owner = owner

class ConfirmConnectionMessage(BaseMessage):
    """
    To confirm the connection. 
    Make sure the node can send currect message to 
    """
    
    __slots__ = ["port"]
    
    def __init__(self, owner: "BaseNode", port: T_Port, *, data: Any = None) -> None:
        if isinstance(port, Port):
            self.port = port
        else:
            self.port = Port(port)
        self.owner = owner

class PullRequestMessage(BaseMessage):
    """
    Be sent when a node is activated but does not have data
    required. The node will be hung up till all the data ready.
    """

    __slots__ = []

    def __init__(self, owner: "Node") -> None:
        self.owner = owner
    
class Message(BaseMessage):
    """
    Input from other node.
    """
    
    __slots__ = ["port"]

    def __init__(self, owner: "BaseNode", port: T_Port, *, data: Any) -> None:
        if isinstance(port, Port):
            self.port = port
        else:
            self.port = Port(port)
        super().__init__(owner, data=data)

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

"""
========================================================

Node Part

========================================================
"""

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

class Node(BaseNode):
    """
    Normal nodes which we used.
    """

    __slots__ = ["requests", "ops", "ports", "data_cache"]

    def __init__(
        self,
        name: str,
        connections: Dict[str, T_Port] = {}, 
        requests: Dict[str, T_Port] = {},
        *,
        network: Optional["BaseNetwork"] = None
    ) -> None:
        self.name       = name
        self.requests   = {k:Port(v) for k,v in requests.items()}
        self.connections= {k:Port(v) for k,v in connections.items()}

        self.network    = network
        self.data_cache: Dict[str, Deque[Any]] = defaultdict(deque)

    async def __respond__(self, msg: BaseMessage) -> None:
        handler = self.__class__._get_message_handler(msg)
        if handler:
            ans = await handler(self, msg)
            if not ("return" in handler.__annotations__ and handler.__annotations__.get("return", None) == None):
                self.deliver(ans)
        else:
            raise InvalidMessageError(f"node {self} failed to receive message {msg}", message=msg)
            
    @on_message(NetwortInitedMessage)
    async def __respond_netinit(self, msg: NetwortInitedMessage) -> None:
        self.network = msg.network
        if not msg.lock.locked():
            await msg.lock.acquire()

        for node, port in self.requests.items():
            c_msg = ConnectionRequestMessage(self, port)
            await c_msg.aio_send(node)
        for node, port in self.connections.items():
            c_msg = ConfirmConnectionMessage(self, port)
            await c_msg.aio_send(node)
        
        msg.uninit.remove(self.name)
        if not msg.uninit:
            msg.lock.release()
    
    @on_message(ConnectionRequestMessage)
    def __respond_connection(self, msg: ConnectionRequestMessage) -> None:
        node: str = msg.owner.name
        if node in self.connections and self.connections[node] != msg.port:
            raise VnodeError("Unmatch connection port.")
        else:
            self.connections[node] = msg.port
    
    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        node: str = msg.owner.name
        if node in self.requests and self.requests[node] != msg.port:
            raise VnodeError("Unmatch connection port.")
        else:
            self.requests[node] = msg.port
            msg.port.complement(self)
    
    @on_message(PullRequestMessage)
    def __respond_pull_request(self, msg: PullRequestMessage) -> None:
        pass
    
    @on_message(InputMessage)
    async def __respond_input(self, msg: InputMessage) -> Any:
        return await msg.aio_bind(self.__run__)
    
    @on_message(Message)
    async def __respond_message(self, msg: Message) -> Any:
        port = msg.port
        port.complement(self)

        if self.ports[0] == "@void":
            return await self.__run__()
        elif len(self.ports) == 1:
            return await self.__run__(msg.data)
        else:
            self.data_cache[port.name].appendleft(msg.data)
            for n, p in self.requests.items():
                if not self.data_cache[p.name]:
                    m = PullRequestMessage(self)
                    await m.aio_send(n)
            if len(self.data_cache) == len(self.ports) and all((self.data_cache.values())):
                return await self.__run__(**{k:v.pop() for k, v in self.data_cache.items()})
    
    def deliver(self, data: Any) -> None:
        if not self.connections:
            return
        for n, p in self.connections.items():
            msg = Message(self, Port(p), data=data)
            msg.send(n)
    
    def set_ops(self, ops: Callable) -> None:
        sig = signature(ops).parameters
        self.ports = [n for n in sig if n != "return"]
        if not self.ports:
            self.ports.append("@void")
        for p in self.requests.values():
            p.complement(self)
        self.ops = ops

    async def __run__(self, *args, **kwds):
        sig = signature(self.ops).parameters
        if sig and list(sig.keys())[0] == "node_self" and sig["node_self"] == self.__class__:
            args = (self, *args)
        if iscoroutinefunction(self.ops):
            return await self.ops(*args, **kwds)
        else:
            return self.ops(*args, **kwds)

class StaticNode(Node):
    """
    The kind of nodes is usually used to store some data.
    When request is received, data will be directly sent out.
    """

    __slots__ = ["data"]

    def __init__(
        self,
        name: str,
        requests: Dict[str, T_Port] = {},
        *,
        network: Optional["BaseNetwork"] = None,
        data: Any = None
    ) -> None:
        self.name       = name
        self.requests   = {k:Port(v) for k,v in requests.items()}
        self.connections= {}

        self.network    = network
        self.data       = data
    
    @on_message(ConnectionRequestMessage)
    def __respond_connection(self, msg: ConnectionRequestMessage) -> None:
        pass
    
    @on_message(PullRequestMessage)
    def __respond_pull_request(self, msg: PullRequestMessage) -> None:
        node = msg.owner
        port: Port = node.requests[self.name]
        node.data_cache[port.name].appendleft(self.data)

    @on_message(InputMessage)
    def __respond_input(self, msg: InputMessage) -> None:
        if len(msg.args) != 1 or msg.kwds:
            raise VnodeError("Static node only have one port.")
        self.data = msg.args[0]
    
    @on_message(Message)
    async def __respond_message(self, msg: Message) -> None:
        self.data = msg.data
    
    def deliver(self, data: Any) -> None:
        pass

def node(name: str, connections: Dict[str, T_Port] = {}, requests: Dict[str, T_Port] = {}) -> Callable[[Callable], Node]:
    n = Node(name, connections, requests)
    def wrapper(ops: Callable) -> Node:
        n.set_ops(ops)
        return n
    return wrapper

class ListenerNode(Node):

    __slots__ = ["event"]
    
    def __init__(
        self, 
        name: str,
        event: BaseEvent,
        connections: Dict[str, T_Port] = {}, 
        *, 
        network: Optional["BaseNetwork"] = None
    ) -> None:
        self.event = event
        super().__init__(name, connections=connections, network=network)
    
    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        raise VnodeError("Listener cannot be connected.")
    
    @on_message(Message)
    def __respond_message(self, msg: Message) -> None:
        raise VnodeError("Listener cannot be connected.")
    
    def respond(self, msg: BaseMessage) -> None:
        if not self.network:
            raise VnodeError("Failed to link with network.")
        if isinstance(msg, BaseEvent) or issubclass(msg.__class__, BaseEvent):
            self.network.task(self.__run__(msg))
        else:
            self.network.task(self.__respond__(msg))
    
    async def __run__(self, event: BaseEvent) -> None:
        ans = await super().__run__(event)
        self.deliver(ans)

"""
========================================================

Network Part

========================================================
"""

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

class Network(BaseNetwork):

    __slots__ = ["listener"]

    def __init__(self, *nodes: Node, loop: Optional[asyncio.BaseEventLoop] = None) -> None:
        super().__init__(*nodes, loop=loop)
        self.listener: Dict[str, List[ListenerNode]] = {}
    
    def broadcast(self, event: BaseEvent) -> None:
        for n in self.listener[event.name]:
            n.respond(event)