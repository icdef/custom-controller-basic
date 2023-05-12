import threading
from kubernetes import client, config, watch
from typing import Dict

from LocalScheduler import LocalScheduler


def start_controller():
    # use for local testing
    # config.load_kube_config()
    # use for the docker image
    config.load_incluster_config()
    supported_zone = "zoneA"
    group = "example.com"
    plural = "localschedulers"
    v2 = client.CustomObjectsApi()
    storage_schedulers: Dict[str, LocalScheduler] = {}

    while True:
        print("Start watching for custom local scheduler for {}".format(supported_zone))
        # wirft ein 404 error wenn die resource noch net online ist.
        # Possible solution: davor checken ob sie existiert und wenn nicht mit api erstellen

        stream = watch.Watch().stream(v2.list_cluster_custom_object, label_selector='zone={}'.format(supported_zone),
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
            zone: str = metadata['labels']['zone']

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


if __name__ == '__main__':
    start_controller()
