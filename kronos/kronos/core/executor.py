from kronos.common.concurrent import GIPCExecutor
from kronos.common.concurrent import GreenletExecutor
from kronos.conf import settings


gipc_executor = GIPCExecutor(
  num_procs=settings.node.gipc_pool_size,
  num_greenlets=settings.node.greenlet_pool_size)

greenlet_executor = GreenletExecutor(
  num_greenlets=settings.node.greenlet_pool_size)
