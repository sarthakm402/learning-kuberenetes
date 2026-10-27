from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

config.load_kube_config()

core = client.CoreV1Api()

NAMESPACE = "default"
LABEL_SELECTOR = "app=chaos-api"


def observe():
    watcher = watch.Watch()

    print("Watching chaos-api Pods...")
    print("Press Ctrl+C to stop.\n")

    try:
        for event in watcher.stream(
            core.list_namespaced_pod,
            namespace=NAMESPACE,
            label_selector=LABEL_SELECTOR,
            timeout_seconds=300
        ):
            event_type = event["type"]
            pod = event["object"]

            print(f"\nEvent: {event_type}")
            print(f"Pod: {pod.metadata.name}")
            print(f"Phase: {pod.status.phase}")

            for container in pod.status.container_statuses or []:
                print(f"Container: {container.name}")
                print(f"Ready: {container.ready}")
                print(f"Restart count: {container.restart_count}")

    except ApiException as exc:
        print(f"Kubernetes API error: {exc}")

    except KeyboardInterrupt:
        print("\nStopping observer...")

    finally:
        watcher.stop()


if __name__ == "__main__":
    observe()