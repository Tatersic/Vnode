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
        connections: Dict[str, T_Port], 
        *,
        requests: Dict[str, T_Port] = {},
        network: Optional[BaseNetwork]
    ) -> None:
        self.name       = name
        self.requests   = {k:Port(v) for k,v in requests.items()}
        self.connections = {k:Port(v) for k,v in connections.items()}
        #self.connections= connections
        self.network    = network
        self.pull_message_cache: Set[PullRequestMessage] = set()

    @on_message(NetwortInitedMessage)
    def __respond_netinit(self, msg: NetwortInitedMessage) -> None:
        self.network = msg.network
        if not self.requests:
            return
        for node, port in self.requests.items():
            c_msg = ConnectionRequestMessage(self, port)
            c_msg.send(node)
    
    @on_message(InputMessage)
    async def __respond_input(self, msg: InputMessage) -> Any:
        return await msg.aio_bind(self.__run__)
    
    @on_message(PullRequestMessage)
    def __respond_pull_requset(self, msg: PullRequestMessage) -> None :
        self.pull_message_cache.add(msg)
    
    @on_message(Message)
    async def __respond_message(self, msg: Message) -> Any:
        data = msg.data
        msg.port.complement(self)

        if len(self.ports) == 1:
            return await self.__run__(data)
        else:
            param_queue = asyncio.Queue(len(self.ports) - 1)
            l_arg: Dict[str, Message] = {}
            for node in (p for p in self.ports if p != msg.port.name):
                m = PullRequestMessage(self, param_queue)
                m.send(node)

            while True:
                rec: Message = await param_queue.get()
                port = rec.port
                port.complement(self)
                if port.name in l_arg:
                    raise VnodeError("unclear input to {0}")
                l_arg[port.name] = rec

                if set(l_arg.keys()) == set(self.ports):
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
                super().deliver(msg)
    
    def set_ops(self, ops: Callable) -> None:
        sig = signature(ops).parameters
        self.ports = [n for n in sig if n != "return"]
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