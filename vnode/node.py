import asyncio
import sys
from inspect import signature
from asyncio.coroutines import iscoroutinefunction
from typing import Callable, Dict, List, Optional, Set, Tuple

from base import VnodeObject, BaseNode, BaseNetwork, BaseMessage, NetwortInitedMessage, on_message
from message import *
from exceptions import *

class Node(BaseNode):
    """
    Normal nodes which we used.
    """

    __slots__ = ["requests", "ops", "ports", "pull_message_cache"]

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
        self.connections = {k:Port(v) for k,v in connections.items()}
        #self.connections= connections
        self.network    = network
        self.pull_message_cache: Set[PullRequestMessage] = set()

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
    
    @on_message(ConfirmConnectionMessage)
    def __respond_confirm_connection(self, msg: ConfirmConnectionMessage) -> None:
        node: str = msg.owner.name
        if node in self.requests and self.requests[node] != msg.port:
            raise VnodeError("Unmatch connection port.")
        else:
            self.requests[node] = msg.port
    
    @on_message(InputMessage)
    async def __respond_input(self, msg: InputMessage) -> Any:
        return await msg.aio_bind(self.__run__)
    
    @on_message(PullRequestMessage)
    def __respond_pull_requset(self, msg: PullRequestMessage) -> None:
        self.pull_message_cache.add(msg)
    
    @on_message(Message)
    async def __respond_message(self, msg: Message) -> Any:
        data = msg.data
        msg.port.complement(self)

        if self.ports[0] == "@void":
            return await self.__run__()
        elif len(self.ports) == 1:
            return await self.__run__(data)
        else:
            param_queue = asyncio.Queue(len(self.ports) - 1)
            l_arg: Dict[str, Message] = {msg.port.name: msg}
            for node, port in {k:v for k, v in self.requests.items() if v != msg.port}.items():
                m = PullRequestMessage(self, param_queue)
                m.send(node)

            while True:
                rec: Message = await param_queue.get()
                port = rec.port
                port.complement(self)
                if port.name in l_arg:
                    raise VnodeError("unclear input to {0}")
                l_arg[port.name] = rec

                if len(l_arg) == len(self.ports):
                    break
            
            arguments = {k:v.data for k, v in l_arg.items()}
            return await self.__run__(**arguments)
    
    def deliver(self, data: Any) -> None:
        if not self.connections:
            return
        for n, p in self.connections.items():
            msg = Message(self, Port(p), data=data)
            for request in self.pull_message_cache:
                if n == request.owner.name:
                    request.append(msg)
                    break
            else:
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

class StaticNode(BaseNode):
    """
    The kind of nodes is usually used to store some data.
    When request is received, data will be directly sent out.
    """

    __slots__ = []

def node(name: str, connections: Dict[str, T_Port] = {}, requests: Dict[str, T_Port] = {}) -> Callable[[Callable], Node]:
    n = Node(name, connections, requests)
    def wrapper(ops: Callable) -> Node:
        n.set_ops(ops)
        return n
    return wrapper