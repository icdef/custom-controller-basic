import logging
import threading
import time
from typing import Dict, Tuple, List

from examples.basic.main import setup_metrics, setup_daemon
from faas.system import FunctionImage, Function, FunctionContainer, FunctionDeployment, ScalingConfiguration, \
    DeploymentRanking
from faas.util.constant import function_label
from faasopts.autoscalers.api import BaseAutoscaler
from faasopts.autoscalers.k8s.hpa.central.latency import HorizontalLatencyPodAutoscaler, \
    HorizontalLatencyPodAutoscalerParameters
from faasopts.loadbalancers.wrr.wrr import SmoothLrtWeightCalculator, RoundRobinWeightCalculator
from galileofaas.connections import RedisClient, KubernetesClient
from galileofaas.context.daemon import GalileoFaasContextDaemon
from galileofaas.context.model import GalileoFaasContext
from galileofaas.system.core import GalileoFaasMetrics, KubernetesResourceConfiguration, KubernetesFunctionDeployment
from galileofaas.system.faas import GalileoFaasSystem
from kubernetes import client, config, watch
from skippy.core.model import ResourceRequirements

from LocalScheduler import LocalScheduler
from loadbalancer import K8sLoadBalancer
from reconcile import K8sReconciliationOptimizationDaemon

logger = logging.getLogger(__name__)


def start_controller():
    # use for local testing
    # config.load_kube_config()
    # use for the docker image
    # config.load_incluster_config()
    KubernetesClient.from_env()
    supported_zone = "zone-a"
    group = "example.com"
    plural = "localschedulers"
    v2 = client.CustomObjectsApi()
    storage_schedulers: Dict[str, LocalScheduler] = {}

    rds = RedisClient.from_env()
    daemon, faas_system, metrics = setup_galileo_faas(rds)
    ctx = daemon.context

    function_kubernetes_deployment = generate_mobilenet_deployment()

    faas_system.deploy(function_kubernetes_deployment)
    print(faas_system.get_replicas('mobilenet'))

    # faas_system.scale_down('mobilenet', faas_system.get_replicas('mobilenet'))
    daemon.context.telemc.unpause_all()
    # start the subscribers to listen for telemetry, traces and Pods
    daemon.start()
    time.sleep(3)

    scaler, load_balancers = setup_orchestration(faas_system, ctx, metrics)
    load_balancer_daemons = []
    for load_balancer in load_balancers:
        def run(var=load_balancer):
            while True:
                time.sleep(1)
                var.update()

        t = threading.Thread(target=run)
        t.start()
        load_balancer_daemons.append(t)

    scaler_daemon = K8sReconciliationOptimizationDaemon(5, scaler)
    scaler_daemon.start()

    print(ctx.telemetry_service.node_resources)
    nodes = daemon.context.node_service.get_nodes()
    print(f'Available nodes: {[n.name for n in nodes]}')
    cpu = daemon.context.telemetry_service.get_node_cpu(nodes[0].name)
    print(f'Mean CPU usage of node {nodes[0].name}: {cpu["value"].mean()}')
    ram = daemon.context.telemetry_service.get_node_ram(nodes[0].name)
    print(f'Mean RAM usage of node {nodes[0].name}: {ram["value"].mean()}')
    time.sleep(1)
    counter = 0
    while counter < 5:
        faas_system.scale_up('mobilenet', 1)
        cpu = daemon.context.telemetry_service.get_node_cpu(nodes[0].name)
        cpu2 = daemon.context.telemetry_service.get_node_cpu(nodes[1].name)
        print(f'Mean CPU usage of node {nodes[0].name}: {cpu["value"].mean()}')
        print(f'Mean CPU usage of node {nodes[1].name}: {cpu2["value"].mean()}')
        ram = daemon.context.telemetry_service.get_node_ram(nodes[0].name)
        ram2 = daemon.context.telemetry_service.get_node_ram(nodes[1].name)
        print(f'Mean RAM usage of node {nodes[0].name}: {ram["value"].mean()}')
        print(f'Mean RAM usage of node {nodes[1].name}: {ram2["value"].mean()}')
        counter += 1
        time.sleep(2)

    time.sleep(20)
    daemon.context.telemc.pause_all()

    daemon.stop(timeout=5)
    scaler_daemon.stop()
    for load_balancer in load_balancer_daemons:
        load_balancer.join(timeout=5)

    rds.close()
    print('we made it?')

    while True:
        print("Start watching for custom local scheduler for {}".format(supported_zone))
        # wirft ein 404 error wenn die resource noch net online ist.
        # Possible solution: davor checken ob sie existiert und wenn nicht mit api erstellen

        stream = watch.Watch().stream(v2.list_cluster_custom_object,
                                      label_selector='ether.edgerun.io/zone={}'.format(supported_zone),
                                      group=group, version="v1", plural=plural)
        for event in stream:
            # print("Event triggered: %s" % event)
            obj = event["object"]
            # there are 3 operations ADDED, MODIFIED, DELETED
            operation: str = event['type']
            spec: Dict = obj.get("spec")
            metadata: Dict = obj.get("metadata")
            scheduler_name: str = metadata['name']
            global_scheduler_name = spec['globalScheduler']
            zone: str = metadata['labels']['ether.edgerun.io/zone']

            if operation == "ADDED":
                print_resource_info(scheduler_name, zone, spec)
                local_scheduler = create_new_scheduler(scheduler_name, zone, global_scheduler_name)
                storage_schedulers[scheduler_name] = local_scheduler
            if operation == "MODIFIED":
                print_resource_info(scheduler_name, zone, spec)
                modify_existing_scheduler(storage_schedulers[scheduler_name], spec)
            if operation == "DELETED":
                create_poison_pod_for_scheduler(scheduler_name)
                storage_schedulers.pop(scheduler_name)
                print("Resource %s was deleted" % scheduler_name)


def generate_mobilenet_deployment():
    function_image = FunctionImage(image='edgerun/mobilenet-inference:1.0.0')
    mobilenet_function = Function(name='mobilenet', fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                        function_label: 'mobilenet'})
    resource_requirements = ResourceRequirements.from_str("1Gi", "1")
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config)
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration()
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name='mobilenet'
                                                                  , namespace='default')
    return function_kubernetes_deployment


def print_resource_info(resource_name: str, zone: str, spec: Dict):
    output_string = "Resource %s in zone %s with: " % (resource_name, zone)
    for key, value in spec.items():
        output_string += "%s: %s " % (key, value)
    print(output_string[:-1])


def modify_existing_scheduler(local_scheduler: LocalScheduler, spec: Dict):
    local_scheduler.global_scheduler_name = spec['globalScheduler']
    print("Scheduler %s updated %s" % (local_scheduler.scheduler_name, spec))


def create_new_scheduler(scheduler_name: str, zone: str, global_scheduler_name: str) -> LocalScheduler:
    local_scheduler = LocalScheduler(scheduler_name, zone, global_scheduler_name)
    # name must be unique for schedulers
    t1 = threading.Thread(target=local_scheduler.start_schedule)

    t1.start()
    return local_scheduler


def create_poison_pod_for_scheduler(scheduler_name: str):
    v1 = client.CoreV1Api()
    pod_metadata = client.V1ObjectMeta()
    pod_metadata.name = "poison-pod"

    container = client.V1Container(name='empty', image='alpine')
    pod_spec = client.V1PodSpec(containers=[container])
    pod_spec.scheduler_name = scheduler_name
    pod_body = client.V1Pod(metadata=pod_metadata, spec=pod_spec, kind='Pod', api_version='v1')

    v1.create_namespaced_pod(namespace='default', body=pod_body)


def setup_galileo_faas(rds: RedisClient) -> Tuple[GalileoFaasContextDaemon, GalileoFaasSystem, GalileoFaasMetrics]:
    metrics = setup_metrics()
    daemon = setup_daemon(rds, metrics)

    faas_system = GalileoFaasSystem(daemon.context, metrics)
    return daemon, faas_system, metrics


def setup_orchestration(faas_system: GalileoFaasSystem, ctx: GalileoFaasContext, metrics: GalileoFaasMetrics) -> Tuple[
    BaseAutoscaler, List[K8sLoadBalancer]]:
    scaler_parameters = {
        # function name: HorizontalLatencyPodAutoscalerParameters
        'mobilenet': HorizontalLatencyPodAutoscalerParameters(lookback=10, target_time_measure='rtt',
                                                              target_duration=39)
    }
    scaler = HorizontalLatencyPodAutoscaler(scaler_parameters, ctx, faas_system, metrics, lambda: time.time())

    # smooth_weight_calculator = SmoothLrtWeightCalculator(ctx, lambda: time.time(), 1)
    round_robin_calculator_a = RoundRobinWeightCalculator()
    round_robin_calculator_b = RoundRobinWeightCalculator()
    load_balancer_a = K8sLoadBalancer(ctx, 'zone-a', metrics, round_robin_calculator_a)
    load_balancer_b = K8sLoadBalancer(ctx, 'zone-b', metrics, round_robin_calculator_b)
    return scaler, [load_balancer_a, load_balancer_b]


def find_function_replicas(fn_name: str) -> List[str]:
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default', pretty=True)
    replica_ids = []
    for pod in pods.items:
        if fn_name in pod.spec.containers[0].image:
            replica_ids.append(pod.metadata.uid)

    return replica_ids


if __name__ == '__main__':
    start_controller()
