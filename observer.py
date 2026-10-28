from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

config.load_kube_config()

core = client.CoreV1Api()

NAMESPACE = "default"
LABEL_SELECTOR = "app=chaos-api"

previous_state = {}


def inspect_pod(pod):
    pod_name = pod.metadata.name
    phase = pod.status.phase

    print(f"\nPod: {pod_name}")
    print(f"Phase: {phase}")

    if phase == "Failed":
        print("ALERT: Pod has failed")

    for container in pod.status.container_statuses or []:
        key = (pod_name, container.name)

        current = {
            "restart_count": container.restart_count,
            "ready": container.ready,
        }

        previous = previous_state.get(key)

        if previous is None:
            print(
                f"Initial state: ready={current['ready']}, "
                f"restarts={current['restart_count']}"
            )

        else:
            if current["restart_count"] > previous["restart_count"]:
                print(
                    f"ALERT: Container {container.name} restarted "
                    f"({previous['restart_count']} -> "
                    f"{current['restart_count']})"
                )

            if previous["ready"] and not current["ready"]:
                print(f"ALERT: Container {container.name} became unready")

            if not previous["ready"] and current["ready"]:
                print(f"RECOVERY: Container {container.name} is ready again")

        previous_state[key] = current


def observe():
    watcher = watch.Watch()

    print("Monitoring chaos-api Pods...")
    print("Press Ctrl+C to stop.")

    try:
        for event in watcher.stream(
            core.list_namespaced_pod,
            namespace=NAMESPACE,
            label_selector=LABEL_SELECTOR,
            timeout_seconds=300,
        ):
            event_type = event["type"]
            pod = event["object"]

            if event_type == "DELETED":
                print(f"\nPod deleted: {pod.metadata.name}")

                for key in list(previous_state):
                    if key[0] == pod.metadata.name:
                        del previous_state[key]

                continue

            if event_type == "ERROR":
                print(f"\nWatch error: {pod}")
                continue

            inspect_pod(pod)

    except ApiException as exc:
        print(f"Kubernetes API error: {exc}")

    except KeyboardInterrupt:
        print("\nStopping observer...")

    finally:
        watcher.stop()


if __name__ == "__main__":
    observe()