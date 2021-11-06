from traceback import print_exception
from typing import Any

from .log import logger
from .node import ExceptionRaisedEvent, node_model
from .node import Node, ListenerNode, node, builtin_node, listener

@builtin_node
@node_model
def __start__(node_self: Node) -> None:
    logger.info(f"{node_self.network} starts running.")

@builtin_node
@node_model
def __return__(node_self: Node, input: Any) -> None:
    node_self.network.last_output = input

@builtin_node
@listener(ExceptionRaisedEvent, model=True)
def __except__(node_self: ListenerNode, event: ExceptionRaisedEvent) -> None:
    logger.error("When the network running, an exception raised.")
    print_exception(event.exception.__class__, event.exception, event.traceback)
    if len(node_self.network.listener[ExceptionRaisedEvent.__name__]) < 2:
        node_self.network.last_output = None

__all__ = [
    "__start__",
    "__return__",
    "__except__"
]