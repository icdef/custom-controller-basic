import time
from threading import Thread

from faas.opt.api import Optimizer
from faas.opt.reconciled import ReconciliationOptimizationDaemon


class K8sReconciliationOptimizationDaemon(ReconciliationOptimizationDaemon):

    def __init__(self, interval: float, optimizer: Optimizer):
        super().__init__(optimizer)
        self.interval = interval

    def sleep(self):
        time.sleep(self.interval)

    def start(self):
        t = Thread(target=self.run)
        t.start()
        return t
