import asyncio
from typing import Dict, List, Optional

from base import BaseMessage, BaseNetwork, on_message
from message import T_Port, ConfirmConnectionMessage, Message
from node import Node
from exceptions import *

class BaseEvent(BaseMessage):

    __slots__ = []

    @property
    def name(self) -> str:
        return self.__class__.__name__

class ListenerNode(Node):

    __slots__ = ["event"]
    
    def __init__(
        self, 
        name: str,
        event: BaseEvent,
        connections: Dict[str, T_Port] = {}, 
        *, 
        network: Optional[BaseNetwork] = None
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

class Network(BaseNetwork):

    __slots__ = ["listener"]

    def __init__(self, *nodes: Node, loop: Optional[asyncio.BaseEventLoop] = None) -> None:
        super().__init__(*nodes, loop=loop)
        self.listener: Dict[str, List[ListenerNode]] = {}
    
    def broadcast(self, event: BaseEvent) -> None:
        for n in self.listener[event.name]:
            n.respond(event)