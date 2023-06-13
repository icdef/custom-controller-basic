import json
import logging
from typing import Dict

from faasopts.loadbalancers.wrr.wrr import WrrOptimizer
from galileofaas.connections import EtcdClient

logger = logging.getLogger(__name__)


class K8sLoadBalancer(WrrOptimizer):

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        # look at galileoexperiments.utils.helpers.set_weights
        print(weights)
        client = EtcdClient.from_env()

        logger.warning('implement me (loadbalancer.set_weights, call etcd')
        pass

    # def set_weights_rr(pods: List[Pod], cluster: str, fn: str):
    #     client = EtcdClient.from_env()
    #     weights = {
    #         "ips": [f'{pod.ip}:8080' for pod in pods],
    #         "weights": [1] * len(pods)
    #     }
    #     key = f'golb/function/{cluster}/{fn}'
    #     value = json.dumps(weights)
    #     logger.info(f'Set following in etcd {key} - {value}')
    #     client.write(key=key, value=value)
    #     return key
    #
    # def set_weight(pod: Pod, weight: int):
    #     client = EtcdClient.from_env()
    #     zone = pod.labels[zone_label]
    #     fn = pod.labels[function_label]
    #     ip = pod.ip
    #     key = f'golb/function/{zone}/{fn}'
    #     value = json.dumps({"ips": [f'{ip}:8080'], "weights": [weight]})
    #     client.write(key=key, value=value)
