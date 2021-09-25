from typing import List, Union, Optional, Any, TypeVar

from base import VnodeObject, BaseMessage, InputMessage, BaseNode

class Port(VnodeObject):

    __slots__ = ["id", "name"]

    id: Optional[int]
    name: Optional[str]

    def __init__(self, id_or_name: Union[int, str, "Port"], *, node: Optional[BaseNode] = None) -> None:
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
        if (not self.name) and self.id != None:
            self.name = ports[self.id]
        else:
            self.id = ports.index(self.name)

T_Port = TypeVar("T_Port", Port, int, str)

class ConnectionRequestMessage(BaseMessage):
    """
    When this message is sent to node A from node B,
    node A should conect to node B.
    """
    
    __slots__ = ["port"]
    
    def __init__(self, owner: BaseNode, port: T_Port, *, data: Any = None) -> None:
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
    
    def __init__(self, owner: BaseNode, port: T_Port, *, data: Any = None) -> None:
        if isinstance(port, Port):
            self.port = port
        else:
            self.port = Port(port)
        self.owner = owner

class Message(BaseMessage):
    """
    Input from other node.
    """
    
    __slots__ = ["port"]

    def __init__(self, owner: BaseNode, port: T_Port, *, data: Any) -> None:
        if isinstance(port, Port):
            self.port = port
        else:
            self.port = Port(port)
        super().__init__(owner, data=data)

__all__ = [
    "Message",
    "InputMessage",
    "ConnectionRequestMessage",
    "ConfirmConnectionMessage",
    "Port",
    "T_Port"
]