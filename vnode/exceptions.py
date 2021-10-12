from typing import Any, Optional

class VnodeError(Exception):
    """
    Base error class of all vnode errors.
    """

    __slots__ = []

class VnodeRuntimeError(VnodeError):

    __slots__ = ["node"]
    
    def __init__(self, *args: object, node=None) -> None:
        self.node = node
        super().__init__(*args)

class NodeTerminatedError(VnodeRuntimeError):

    __slots__ = ["data"]

    def __init__(self, *args: object, node=None, data=None) -> None:
        self.data = data
        super().__init__(*args, node=node)

class VnodeValueError(VnodeError):

    __slots__ = ["node"]

    def __init__(self, *args: object, node=None) -> None:
        self.node  = node
        super().__init__(*args)

class InvalidMessageError(VnodeError):
    """
    When a node reveive the message which cannot be handled,
    the error will be raised.
    """
    __slots__ = ["message"]

    def __init__(self, *args, message: Optional[Any] = None) -> None:
        self.message = message
        super().__init__(*args)

class MissingNetworkError(VnodeValueError):

    __slots__ = []

class VnodePortError(VnodeValueError):

    __slots__ =[]

class VnodeNetworkError(VnodeValueError):

    __slots__ = ["network"]

    def __init__(self, *args: object, net=None) -> None:
        self.network = net
        super().__init__(*args)