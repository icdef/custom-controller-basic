import json
import random
from typing import List, Dict

from kubernetes import client, watch


class LocalScheduler:
    def __init__(self, scheduler_name: str, zone: str, global_scheduler_name=""):
        self.scheduler_name = scheduler_name
        self.v1 = client.CoreV1Api()
        self.v2 = client.AppsV1Api()
        self.zone = zone
        self.global_scheduler_name = global_scheduler_name

    def __str__(self):
        return f"Scheduler: {self.scheduler_name}"

    def get_deployment_name(self, owner_references):
        if isinstance(owner_references, list):
            owner_name = owner_references[0].name
            owner_kind = owner_references[0].kind
            if owner_kind == 'ReplicaSet':
                replica_set = self.v2.read_namespaced_replica_set(name=owner_name, namespace="default")
                owner_references2 = replica_set.metadata.owner_references
                if isinstance(owner_references2, list) and owner_references2[0].kind == 'Deployment':
                    return owner_references2[0].name
                else:
                    return None
            else:
                return None

    def nodes_available(self) -> List[str]:
        ready_nodes = []
        for n in self.v1.list_node(label_selector='zone={}'.format(self.zone)).items:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ready_nodes.append(n.metadata.name)
        print(ready_nodes)
        return ready_nodes

    def schedule_pod(self, pod_name: str, node_name: str, namespace="default"):
        target = client.V1ObjectReference()
        target.kind = "Node"
        target.apiVersion = "v1"
        target.name = node_name

        meta = client.V1ObjectMeta()
        meta.name = pod_name

        body = client.V1Binding(target=target, metadata=meta)

        # looks like to be a lib error: https://github.com/kubernetes-client/python/issues/547
        self.v1.create_namespaced_pod_binding(pod_name, namespace, body, _preload_content=False)
        print("scheduled")

    def reschedule_pod_deployment(self, deployment_name: str, scheduler_name: str):
        # because schedulerName from pods cannot be changed, so we delete the pod to reschedule
        deployment = self.v2.read_namespaced_deployment(name=deployment_name, namespace='default')
        self.v2.delete_namespaced_deployment(name=deployment_name, namespace='default')
        last_applied_configuration_json = deployment.metadata.annotations['kubectl.kubernetes.io/last-applied-configuration']
        deployment_data = json.loads(last_applied_configuration_json)
        # this annotations is normally set only kubectl apply command but we need it for bottom up
        deployment_data['metadata']['annotations'][
            'kubectl.kubernetes.io/last-applied-configuration'] = last_applied_configuration_json
        deployment_spec = deployment_data['spec']
        deployment_spec['template']['spec']['schedulerName'] = scheduler_name
        deployment_copy = client.V1Deployment(api_version=deployment_data['apiVersion'], kind=deployment_data['kind'],
                                              metadata=deployment_data['metadata'], spec=deployment_spec)

        self.v2.create_namespaced_deployment(namespace='default', body=deployment_copy)
        print("rescheduled")

    def start_schedule(self):
        print("Start scheduling %s" % self.scheduler_name)
        running = True
        while running:
            w = watch.Watch()
            stream = w.stream(self.v1.list_namespaced_pod, namespace="default")
            for event in stream:
                pod = event['object']
                if pod.status.phase == "Pending" and pod.spec.scheduler_name == self.scheduler_name and \
                        pod.spec.node_name is None and pod.metadata.deletion_timestamp is None:
                    owner_references = pod.metadata.owner_references
                    deployment_name = self.get_deployment_name(owner_references)
                    if pod.metadata.name == 'poison-pod':
                        w.stop()
                        running = False
                        self.v1.delete_namespaced_pod(name='poison-pod', namespace='default')
                        break
                    print("scheduling pod: %s" % pod.metadata.name)
                    try:
                        nodes_available = self.nodes_available()
                        if not nodes_available and self.global_scheduler_name:
                            self.reschedule_pod_deployment(deployment_name, self.global_scheduler_name)
                        elif nodes_available:
                            self.schedule_pod(pod_name=pod.metadata.name,
                                              node_name=random.choice(nodes_available))
                        else:
                            print("Could not schedule pod: %s" % pod.metadata.name)
                    except client.exceptions.ApiException as e:
                        print(json.loads(e.body)['message'])

        print("End scheduling %s" % self.scheduler_name)
