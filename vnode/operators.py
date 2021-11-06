from typing import Any, Callable, Dict, Iterable, Optional, Union

from .node import VnodeObject, Node
from .exceptions import VnodeError

class Operator(VnodeObject):

    __slots__ = ["name", "func"]

    ops_cahce: Dict[str, "Operator"] = {}
    
    def __init__(self, name: str, func: Callable[..., Callable]) -> None:
        self.name = name
        self.func = func
        if name in self.ops_cahce:
            raise VnodeOpsError(f"Operator '{name}' has been registered.")
        else:
            self.__class__.ops_cahce[name] = self
    
    def __call__(self, *args: Any, **kwds: Any) -> Node:
        func = self.func(*args, **kwds)
        node = Node(self.name)
        node.set_ops(func)
        return node
    
    def __class_getitem__(cls, key: str) -> "Operator":
        return cls.ops_cahce[key]

def vnode_operator(name: Optional[str] = None) -> Callable[[Callable[..., Callable]], Operator]:
    def wrapper(func: Callable[..., Callable]) -> Operator:
        if not name:
            _name = func.__name__
        else:
            _name = name
        return Operator(_name, func)
    return wrapper

@vnode_operator()
def add_constant(constant):
    def wrapper(num):
        return num + constant
    return wrapper

@vnode_operator()
def mul_constant(constant):
    def wrapper(num):
        return num * constant
    return wrapper

@vnode_operator()
def div_constant(constant):
    def wrapper(num):
        return num / constant
    return wrapper

@vnode_operator()
def fma(b, c):
    def wrapper(a):
        return (a + b) * c
    return wrapper

@vnode_operator()
def format_str(string: str):
    def wrapper(args: Iterable, kwds: dict):
        return string.format(*args, **kwds)
    return wrapper

class VnodeOpsError(VnodeError):

    __slots__ = []