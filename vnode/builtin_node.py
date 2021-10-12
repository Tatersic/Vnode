from traceback import print_exception
from typing import Any

from .log import logger
from .node import ExceptionRaisedEvent
from .node import Node, ListenerNode, node, builtin_node, listener

@builtin_node
@node("__start__", model=True)
def _network_start(node_self: Node) -> None:
    logger.info(f"{node_self.network} starts running.")

@builtin_node
@node("__return__", model=True)
def _network_output(node_self: Node, input: Any) -> None:
    node_self.network.last_output = input

@builtin_node
@listener(ExceptionRaisedEvent, "__exception__", model=True)
def _network_except(node_self: ListenerNode, event: ExceptionRaisedEvent) -> None:
    logger.error("When the network running, an exception raised.")
    print_exception(event.exception.__class__, event.exception, event.traceback)
    node_self.network.last_output = None