import json
import logging
from typing import Dict, List

from faasopts.loadbalancers.wrr.wrr import WrrOptimizer
from galileoexperiments.api.model import Pod
from galileoexperiments.utils.helpers import set_weights_rr
from galileoexperiments.utils.k8s import get_pods
from galileofaas.connections import EtcdClient, KubernetesClient

from randomscheduler import pods_in_cluster

logger = logging.getLogger(__name__)


class K8sLoadBalancer(WrrOptimizer):

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        # look at galileoexperiments.utils.helpers.set_weights
        kube_config = KubernetesClient.from_env()
        print(weights)
        cluster = 'zone-a'
        set_weights_rr(get_pods(pods_in_cluster(cluster), kube_config.corev1_api), cluster, 'mobilenet')
        logger.warning('implement me (loadbalancer.set_weights, call etcd')


