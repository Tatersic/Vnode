from typing import Any, Optional

class VnodeError(Exception):
    """
    Base error class of all vnode errors.
    """

    __slots__ = []

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
