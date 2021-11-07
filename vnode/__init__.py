from .node import *
from .builtin_node import *
from .operators import *

async def run(node: Node, *args: Any, **kwds: Any) -> Any:
    """
    Run a node. 
    """
    return await node.__run__(*args, **kwds)