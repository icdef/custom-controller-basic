import json
import logging
from typing import Dict, List

from faasopts.loadbalancers.wrr.wrr import WrrOptimizer
from galileoexperiments.api.model import Pod
from galileoexperiments.utils.helpers import set_weights_rr
from galileoexperiments.utils.k8s import get_pods
from galileofaas.connections import EtcdClient

from randomscheduler import pods_in_cluster

logger = logging.getLogger(__name__)


class K8sLoadBalancer(WrrOptimizer):

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        # look at galileoexperiments.utils.helpers.set_weights
        print(weights)
        cluster = 'zone-a'
        set_weights_rr(get_pods(pods_in_cluster(cluster)), cluster, 'mobilenet')
        logger.warning('implement me (loadbalancer.set_weights, call etcd')


