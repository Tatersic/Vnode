import asyncio
import re
import sys
from asyncio.coroutines import iscoroutinefunction
from collections import OrderedDict, defaultdict, deque
from functools import wraps
from inspect import signature
from types import TracebackType
from typing import (Any, Callable, Coroutine, Deque, Dict, Generator, Iterable, List,
                    NoReturn, Optional)
from typing import OrderedDict as T_OrderedDict
from typing import DefaultDict
from typing import Tuple, Type, TypeVar, Union

from .exceptions import *
from .log import *

NODE_OPS_SELF_REF: str = "node_self"
RE_NODE_NAME = re.compile(r"^[a-zA-Z\_][0-9a-zA-Z\_]*(\.[a-zA-Z\_][0-9a-zA-Z\_]*)*$")

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
            raise MissingNetworkError("Failed to link with network.", node=self.owner)
        net.send(receiver, self)
    
    async def aio_send(self, receiver: str, *, net: Optional["BaseNetwork"] = None) -> None:
        net = net or self.owner.network
        if not net:
            raise MissingNetworkError("Failed to link with network.", node=self.owner)
        await net[receiver].__respond__(self)
    
    @property
    def name(self) -> str:
        return self.__class__.__name__

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

class NetworkInitedMessage(BaseMessage):

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
    
    def complement(self, node: "Node") -> None:
        try:
            ports: list = node.ports
            if (not self.name) and self.id != None:
                self.name = ports[self.id]
            else:
                self.id = ports.index(self.name)
        except (ValueError, IndexError):
            raise VnodeValueError(f"{self} cannot match {node.ports}", node=node)
        except:
            print(self, node)
    
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

    def __init__(self, func: Callable[["BaseNode", BaseMessage], Any], message: Tuple[Type[BaseMessage]]) -> None:
        if not asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(node: BaseNode, msg: BaseMessage):
                return func(node, msg)
            self.func = wrapper
        else:
            self.func = func
        self.message = message

def on_message(*msg: Type[BaseMessage]) -> Callable[[Callable], _OnMessage]:
    def wrapper(func: Callable[["BaseNode", BaseMessage], Any]) -> _OnMessage:
        return _OnMessage(func, msg)
    return wrapper

class BaseEvent(BaseMessage):

    __slots__ = []

    def broadcast(self, net: Optional["Network"] = None) -> None:
        network: Network = net or self.owner.network
        network.broadcast(self)

class ExceptionRaisedEvent(BaseEvent):

    __slots__ = ["exception", "traceback"]

    def __init__(self, owner: "BaseNode", exception: BaseException, traceback: TracebackType) -> None:
        self.exception = exception
        self.traceback = traceback
        super().__init__(owner, data=exception)

class NodeGroupStartingEvent(BaseEvent):

    __slots__ = []

    data: Dict[str, Any]

"""
========================================================

Node Part

========================================================
"""

async def _wait_lock(func: Coroutine, lock: asyncio.Lock) -> Any:
    await lock.acquire()
    await func

class NodeMeta(type):

    __message__: Dict[Tuple[Type[BaseMessage]], Callable[["BaseNode", BaseMessage], Any]]

    def __new__(cls, name: str, bases: tuple, attrs: Dict[str, Any]):
        if "__message__" in attrs:
            raise VnodeValueError("Invalid attribute '__message__'")
        n_attrs = {"__message__":{}}
        if name != "BaseNode":
            for node in bases:
                n_attrs["__message__"].update(node.__message__)
        for n, m in attrs.items():
            if isinstance(m, _OnMessage):
                n_attrs["__message__"][m.message] = m.func
            else:
                n_attrs[n] = m
        return type.__new__(cls, name, bases, n_attrs)

    def _get_message_handler(self, msg: BaseMessage) -> Optional[Callable]:
        for k, v in self.__message__.items():
            if msg.__class__ in k:
                return v
    
    def __instancecheck__(self, instance: Any) -> bool:
        if super().__instancecheck__(instance):
            return True
        else:
            return self.__subclasscheck__(instance.__class__)

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
            raise MissingNetworkError("Failed to link with network.", node=self)
        self.network.task(self.__respond__(msg))
    
    def join(self, net: "BaseNetwork") -> "BaseNode":
        self.network = net
        return self
    
    def copy(self, network: Optional["BaseNetwork"], connections: List[str] = []) -> "BaseNode":
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

    __slots__ = ["requests", "ops", "ports", "data_cache", "model"]

    def __init__(
        self,
        name: str,
        connections: Dict[str, T_Port] = {}, 
        requests: Dict[str, T_Port] = {},
        *,
        network: Optional["Network"] = None,
        model: bool = False
    ) -> None:
        if not RE_NODE_NAME.match(name):
            raise ValueError("Invalid node name.")
        self.name       = name
        self.requests   = {k:Port(v) for k,v in requests.items()}
        self.connections= {k:Port(v) for k,v in connections.items()}

        self.model = model
        if model and network:
            raise VnodeNetworkError("Node models should not be linked to a network.", net=network)
        self.network    = network
        self.data_cache: Dict[str, Deque[Any]] = defaultdict(deque)

    async def __respond__(self, msg: BaseMessage) -> None:
        handler = self.__class__._get_message_handler(msg)
        if handler:
            try:
                ans = await handler(self, msg)
            except NodeTerminatedError:
                pass
            except VnodeError:
                raise
            except:
                event = ExceptionRaisedEvent(self, *sys.exc_info()[1:])
                self.broadcast(event)
            else:
                if not ("return" in handler.__annotations__ and handler.__annotations__.get("return", None) == None):
                    self.deliver(ans)
        else:
            raise InvalidMessageError(f"Node {self} failed to receive message {msg}", message=msg)
            
    @on_message(NetworkInitedMessage)
    async def __respond_netinit(self, msg: NetworkInitedMessage) -> None:
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
            raise VnodePortError("Unmatch connection port.", node=self)
        else:
            self.connections[node] = msg.port
    
    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        node: str = msg.owner.name
        if node in self.requests and self.requests[node] != msg.port:
            raise VnodePortError("Unmatch connection port.", node=self)
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
    
    def copy(
        self,
        network: Optional["Network"],
        connections: Dict[str, T_Port] = {}, 
        requests: Dict[str, T_Port] = {}
    ) -> "Node":
        n_node = self.__class__(
            self.name, connections, requests, network=network
        )
        n_node.set_ops(self.ops)
        return n_node
    
    def join(self, network: "Network") -> "Node":
        if self.network:
            raise VnodeNetworkError(f"Node {self} was reused in differnet networks.", net=network)
        if not self.model:
            self.network = network
            return self
        else:
            return self.copy(network, self.connections, self.requests)

    def deliver(self, data: Any) -> None:
        if not self.connections:
            return
        for n, p in self.connections.items():
            msg = Message(self, Port(p), data=data)
            msg.send(n)
    
    def broadcast(self, event: BaseEvent) -> None:
        self.network.broadcast(event)
    
    def terminate(self, data: Any = None) -> NoReturn:
        raise NodeTerminatedError(f"Node {self} was terminated by user.", node=self, data=data)
    
    def set_ops(self, ops: Callable) -> None:
        sig = signature(ops).parameters
        self.ports = [n for n in sig if n != "return" and n != NODE_OPS_SELF_REF]
        if not self.ports:
            self.ports.append("@void")
        for p in self.requests.values():
            p.complement(self)
        self.ops = ops

    def __call__(self, *args: Any, **kwds: Any) -> None:
        super().__call__(*args, **kwds)
        return self.network.last_output

    async def __run__(self, *args, **kwds) -> Any:
        sig = signature(self.ops)
        empty = sig.empty
        sig = sig.parameters
        if (
                sig and \
                list(sig.keys())[0] == NODE_OPS_SELF_REF and (
                sig[NODE_OPS_SELF_REF].annotation == self.__class__ or 
                issubclass(self.__class__, sig[NODE_OPS_SELF_REF].annotation) or
                sig[NODE_OPS_SELF_REF].annotation == empty)
            ):
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
        model: bool = False,
        data: Any = None
    ) -> None:
        if not RE_NODE_NAME.match(name):
            raise ValueError("Invalid node name.")
        self.name       = name
        self.requests   = {k:Port(v) for k,v in requests.items()}
        self.connections= {}
        
        self.model = model
        if model and network:
            raise VnodeNetworkError("Node models should not be linked to a network.", net=network)
        self.data       = data
        self.ports = ["@data"]
    
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
            raise VnodePortError("Static node only have one port.", node=self)
        self.data = msg.args[0]
    
    @on_message(Message)
    async def __respond_message(self, msg: Message) -> None:
        self.data = msg.data
    
    def copy(
        self,
        network: Optional["Network"],
        connections: Dict[str, T_Port] = {}
    ) -> "StaticNode":
        n_node = self.__class__(
            self.name, connections, network=network
        )
        return n_node
    
    def join(self, network: "Network") -> "StaticNode":
        if not self.model:
            self.network = network
            return self
        else:
            return self.copy(network, self.connections)

    def deliver(self, data: Any) -> None:
        pass

    def set_ops(self, ops: Callable) -> NoReturn:
        raise VnodeValueError("Static node needs no operator.", node=self)

class ListenerNode(Node):

    __slots__ = ["event"]
    
    def __init__(
        self, 
        name: str,
        event: Type[BaseEvent],
        connections: Dict[str, T_Port] = {}, 
        *, 
        network: Optional["Network"] = None,
        model: bool = False
    ) -> None:
        self.event = event
        super().__init__(name, connections=connections, network=network, model=model)
    
    async def __respond__(self, msg: BaseMessage) -> None:
        if isinstance(msg, BaseEvent) or issubclass(msg.__class__, BaseEvent):
            await self.__run__(msg)
        else:
            await super().__respond__(msg)

    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        raise VnodeValueError("Listener cannot be connected.", node=self)
    
    @on_message(Message)
    def __respond_message(self, msg: Message) -> None:
        raise VnodeValueError("Listener cannot be connected.", node=self)
    
    def copy(self, network: Optional["Network"], connections: Dict[str, T_Port] = {}, requests: Dict[str, T_Port] = {}) -> "ListenerNode":
        if requests:
            raise VnodeValueError("")
        n_node: ListenerNode = super().copy(network, connections=connections, requests=requests)
        n_node.event = self.event
        return n_node

    async def __run__(self, event: BaseEvent) -> None:
        ans = await super().__run__(event)
        self.deliver(ans)

class NodeGroup(Node):

    __slots__ = ["ins_network", "subordinate_nodes"]

    def __init__(
        self, 
        name: str, 
        connections: Dict[str, T_Port] = {}, 
        requests: Dict[str, T_Port] = {}, 
        *, 
        network: Optional["Network"] = None, 
        model: bool = False
    ) -> None:
        self.subordinate_nodes: Dict[str, Node] = {}
        self.ports: List[str] = []
        super().__init__(
            name, connections=connections, requests=requests, 
            network=network, model=model)

    def join(self, network: "Network") -> "Node":
        self.ins_network = NodeGroupNetwork(loop=network.loop)
        self.ins_network.add(*self.subordinate_nodes.values())
        return super().join(network)

    def add_nodes(self, *nodes: Node) -> None:
        for n in nodes:
            self.subordinate_nodes[n.name] = n
            if isinstance(n, NodeGroupPort):
                self.ports.insert(n.id, n.name)
        if hasattr(self, "ins_network"):
            self.ins_network.add(*nodes)
    
    def set_ops(self, ops: Callable) -> NoReturn:
        raise VnodeValueError("Node group needs no operator.", node=self)
    
    def copy(self, network: Optional["Network"], connections: Dict[str, T_Port] = {}, requests: Dict[str, T_Port] = {}) -> "NodeGroup":
        n_node: NodeGroup = super().copy(network, connections=connections, requests=requests)
        l_sn = []
        for n in self.subordinate_nodes.values():
            ns_node = n.copy(None, n.connections, n.requests)
            l_sn.append(ns_node)
        n_node.add_nodes(*l_sn)
        return n_node

    async def __run__(self, *args, **kwds) -> Any:
        data = {}
        for k, v in zip(self.ports, args):
            data[k] = v
        data.update(kwds)
        if len(data) != len(self.ports):
            raise VnodeValueError("Unmatch data.", node=self)

        async with self.ins_network:
            NodeGroupStartingEvent(self, data=data).broadcast(self.ins_network)
        return self.ins_network.last_output

class NodeGroupPort(ListenerNode):

    __slots__ = ["id"]

    def __init__(self, id: int, name: str, *, network: Optional["BaseNetwork"] = None) -> None:
        self.id = id
        super().__init__(name, NodeGroupStartingEvent, network=network)

    def copy(self, network: Optional["Network"], connections: Dict[str, T_Port] = {}, requests: Dict[str, T_Port] = {}) -> "NodeGroupPort":
        n_node: NodeGroupPort = super().copy(network, connections=connections, requests=requests)
        n_node.id = self.id
        return n_node

    async def __run__(self, event: NodeGroupStartingEvent) -> None:
        data = event.data[self.name]
        await super().__run__(data)

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
        loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        if loop:
            self.loop: asyncio.AbstractEventLoop = loop
        else:
            self.loop = asyncio.new_event_loop()
    
        self.nodes = OrderedDict()
        for n in nodes:
            self.nodes[n.name] = n.join(self)
        
        self.activated = self.loop.is_running()

    def _init(self, first_task: Optional[Coroutine] = None) -> None:
        self.activated = True
        lock = asyncio.Lock()
        l = list(self.nodes.keys())
        for n in self.nodes.values():
            self.task(n.__respond__(NetworkInitedMessage(self, lock, l)))
        if first_task:
            self.task(first_task)
        if not self.loop.is_running():
            self.loop.run_forever()
    
    def add(self, *nodes: BaseNode) -> None:
        for n in nodes:
            self[n.name] = n.join(self)

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
                raise VnodeNetworkError(f"Node {key} is not in network {self}.", net=self)
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

_builtin_node: List[Node] = []

class Network(BaseNetwork):

    __slots__ = ["listener", "_last_output"]

    def __init__(self, *nodes: Node, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        nodes += tuple(_builtin_node)
        super().__init__(*nodes, loop=loop)
        self.listener: DefaultDict[str, List[ListenerNode]] = defaultdict(list)
        for n in self.nodes.values():
            if isinstance(n, ListenerNode):
                event = n.event.__name__
                self.listener[event].append(n)
    
    def add(self, *nodes: BaseNode) -> None:
        super().add(*nodes)
        for n in self.nodes.values():
            if isinstance(n, ListenerNode):
                event = n.event.__name__
                self.listener[event].append(n)
    
    @property
    def last_output(self) -> Any:
        return self._last_output
    
    @last_output.setter
    def last_output(self, value: Any) -> None:
        self._last_output = value
        self.shutdown()
    
    def send(self, receiver: str, message: BaseMessage) -> None:
        if '.' in receiver:
            raise VnodeValueError("Cannot send message to the node a the node group.")
        else:
            super().send(receiver, message)

    def broadcast(self, event: BaseEvent) -> None:
        for n in self.listener[event.name]:
            n.respond(event)
    
    def shutdown(self) -> None:
        if self.loop.is_running():
            self.loop.stop()
            self.activated = False
        else:
            logger.warning(f"network {self}'s returns are setted after loop stopping.")

    def __call__(self) -> Any:
        return self["__start__"]()

class NodeGroupNetwork(Network):

    __slots__ = ["lock"]

    def __init__(self, *nodes: Node, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        super().__init__(*nodes, loop=loop)
        self.lock = asyncio.Lock(loop=self.loop)

    def _init(self, first_task: Optional[Coroutine] = None) -> None:
        self.activated = True
        if first_task:
            self.task(first_task)

    def shutdown(self) -> None:
        self.activated = False
        if self.lock.locked:
            self.lock.release()
    
    async def __aenter__(self) -> "NodeGroupNetwork":
        await self.lock.acquire()
        lock = asyncio.Lock()
        l = list(self.nodes.keys())
        for n in self.nodes.values():
            n.respond(NetworkInitedMessage(self, lock, l))
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_value: Optional[BaseException],
            exc_trace: Optional[TracebackType]
        ) -> None:
        if not exc_type:
            await self.lock.acquire()
            self.lock.release()
        elif self.lock.locked:
            self.lock.release()


def builtin_node(node: Node) -> Node:
    _builtin_node.append(node)
    return node

def node(
        name: Optional[str] = None,
        connections: Dict[str, T_Port] = {},
        requests: Dict[str, T_Port] = {},
        *,
        model: bool = False
    ) -> Callable[[Callable], Node]:
    def wrapper(ops: Callable) -> Node:
        if not name:
            _name = ops.__name__
        else:
            _name = name
        n = Node(_name, connections, requests, model=model)
        n.set_ops(ops)
        return n
    return wrapper

def node_model(ops: Callable) -> Node:
    return node(model=True)(ops)

def listener(
        event: Type[BaseEvent],
        name: Optional[str] = None,
        connections: Dict[str, T_Port] = {},
        *,
        model: bool = False
    ) -> Callable[[Callable], ListenerNode]:
    def wrapper(func: Callable) -> ListenerNode:
        if not name:
            _name = func.__name__
        else:
            _name = name
        node = ListenerNode(_name, event, connections=connections, model=model)
        node.set_ops(func)
        return node
    return wrapper

_node_group_id = 0

def node_group_port(name: str, id_: Optional[int] = None) -> Callable[[Callable[[Any], Any]], NodeGroupPort]:
    def wrapper(func: Callable[[Any], Any]) -> NodeGroupPort:
        global _node_group_id
        if not id_:
            id = _node_group_id
            _node_group_id += 1
        else:
            id = id_
        node_port =  NodeGroupPort(id, name)
        node_port.set_ops(func)
        return node_port
    return wrapper

def node_group(
        name: Optional[str] = None,
        connections: Dict[str, T_Port] = {},
        requests: Dict[str, T_Port] = {},
        *,
        model: bool = False
    ) -> Callable[[Type[object]], NodeGroup]:
    def wrapper(cls: Type[object]) -> NodeGroup:
        global _node_group_id
        if not name:
            _name = cls.__name__
        else:
            _name = name
        n = NodeGroup(_name, connections, requests, model=model)
        n.add_nodes(*(i for i in cls.__dict__.values() if isinstance(i, Node)))
        _node_group_id = 0
        return n
    return wrapper

def network(network: Type[object]) -> Network:
    l = []
    for v in network.__dict__.values():
        if isinstance(v, BaseNode):
            l.append(v)
    return Network(*l)

__all__ = [
    # Message
    "BaseMessage",
    "InputMessage",
    "NetworkInitedMessage",
    "ConnectionRequestMessage",
    "ConfirmConnectionMessage",
    "PullRequestMessage",
    "Message",
    # Event
    "BaseEvent",
    "ExceptionRaisedEvent",
    "NodeGroupStartingEvent",
    
    # Node
    "NodeMeta",
    "BaseNode",
    "Node",
    "StaticNode",
    "ListenerNode",
    "NodeGroup",
    "NodeGroupPort",

    # Network
    "BaseNetwork",
    "Network",

    # Function
    "on_message",
    "builtin_node",
    "node",
    "node_model",
    "listener",
    "node_group_port",
    "node_group",
    "network"
]