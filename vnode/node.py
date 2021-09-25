from collections import deque, defaultdict
from inspect import signature
from asyncio.coroutines import iscoroutinefunction
from typing import Callable, Dict, List, Optional, Deque

from base import VnodeObject, BaseNode, BaseNetwork, BaseMessage, NetwortInitedMessage, on_message
from message import *
from exceptions import *

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
        network: Optional[BaseNetwork] = None
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

        if not self.requests:
            return
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
            msg.port.complement(self)
    
    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        node: str = msg.owner.name
        if node in self.requests and self.requests[node] != msg.port:
            raise VnodeError("Unmatch connection port.")
        else:
            self.requests[node] = msg.port
    
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
        network: Optional[BaseNetwork] = None,
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