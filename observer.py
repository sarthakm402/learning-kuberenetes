from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

config.load_kube_config()

core = client.CoreV1Api()
app= client.AppsV1Api()
NAMESPACE = "default"
LABEL_SELECTOR = "app=chaos-api"
# LABEL_SELECTOR = ""
previous_deployment_health = None
previous_state = {}
def interpret_termination(terminated):
    reason = terminated.reason
    exit_code = terminated.exit_code

    print("\n[INTERPRETATION]")

    if reason == "OOMKilled":
        print("Container was killed because it exceeded its memory limit.")

    elif reason == "Error" and exit_code == 1:
        print("Application exited with an error.")
        print("Check previous container logs for the underlying cause.")

    elif reason == "Unknown" or exit_code == 255:
        print("Kubernetes could not determine the exact termination cause.")
        print("Investigate node and container runtime events.")

    elif reason == "Completed":
        print("Container exited successfully.")

    else:
        print(f"Unrecognized termination: reason={reason}, exit_code={exit_code}")

def diagnose_restart(pod, container):
    pod_name = pod.metadata.name

    print("\n" + "=" * 60)
    print(f"DIAGNOSTICS: {pod_name}")
    print("=" * 60)

    # 1. Inspect the last terminated container state
    terminated = container.last_state.terminated

    print("\n[1] Previous container state")

    if terminated:
        print(f"Reason:    {terminated.reason}")
        print(f"Exit code: {terminated.exit_code}")
        print(f"Message:   {terminated.message or 'No message recorded'}")
        print(f"Finished:  {terminated.finished_at}")
        interpret_termination(terminated)
    else:
        print("No previous termination details available.")

    # 2. Retrieve logs from the previous container instance
    print("\n[2] Previous container logs")

    try:
        logs = core.read_namespaced_pod_log(
            name=pod_name,
            namespace=NAMESPACE,
            container=container.name,
            previous=True,
            tail_lines=50,
        )
        if isinstance(logs, bytes):
           logs = logs.decode("utf-8", errors="replace")
        if logs.strip():
            print(logs)
        else:
            print("Previous container produced no available logs.")

    except ApiException as exc:
        print(f"Could not retrieve previous logs: {exc.reason}")

    # 3. Retrieve events associated with this Pod
    print("\n[3] Pod events")

    try:
        result = core.list_namespaced_event(
            namespace=NAMESPACE,
            field_selector=f"involvedObject.name={pod_name}",
            limit=50,
        )

        events = sorted(
            result.items,
            key=lambda event: (
                event.last_timestamp
                or event.event_time
                or event.metadata.creation_timestamp
            ),
            reverse=True,
        )

        if not events:
            print("No matching events found.")
        else:
            for event in events[:10]:
                print(
                    f"Type: {event.type} | "
                    f"Reason: {event.reason} | "
                    f"Count: {event.count}"
                )
                print(f"Message: {event.message}")
                print("-" * 40)

    except ApiException as exc:
        print(f"Could not retrieve Pod events: {exc.reason}")

    print("=" * 60 + "\n")


def inspect_pod(pod):
    pod_name = pod.metadata.name
    phase = pod.status.phase

    print(f"\nPod: {pod_name}")
    print(f"Phase: {phase}")
    for container in pod.status.container_statuses or []:
     state = container.state

     if state.waiting:
        print(f"Container waiting: {state.waiting.reason}")
        print(f"Message: {state.waiting.message}")

     elif state.running:
        print(f"Container running since: {state.running.started_at}")

     elif state.terminated:
        print(f"Container terminated: {state.terminated.reason}")

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

            # Investigate an existing restart when monitoring starts.
            if current["restart_count"] > 0:
                diagnose_restart(pod, container)

        else:
            if current["restart_count"] > previous["restart_count"]:
                print(
                    f"ALERT: Container {container.name} restarted "
                    f"({previous['restart_count']} -> "
                    f"{current['restart_count']})"
                )

                diagnose_restart(pod, container)

            if previous["ready"] and not current["ready"]:
                print(
                    f"ALERT: Container {container.name} became unready"
                )

            if not previous["ready"] and current["ready"]:
                print(
                    f"RECOVERY: Container {container.name} is ready again"
                )

        previous_state[key] = current
def check_deployment_health():
    global previous_deployment_health

    deployment = app.read_namespaced_deployment(
        name="chaos-api",
        namespace=NAMESPACE
    )

    desired = deployment.spec.replicas or 0
    ready = deployment.status.ready_replicas or 0

    healthy = ready >= desired

    if previous_deployment_health is None:
        print(
            f"\nInitial Deployment health: "
            f"{'HEALTHY' if healthy else 'DEGRADED'}"
        )

    elif previous_deployment_health and not healthy:
        print("\n[ALERT] Deployment became degraded!")

    elif not previous_deployment_health and healthy:
        print("\n[RECOVERY] Deployment is healthy again!")

    if not healthy:
        print(f"Desired: {desired} | Ready: {ready}")

    previous_deployment_health = healthy
def observe():
    watcher = watch.Watch()

    print("Monitoring chaos-api Pods...")
    print("Automatic diagnostics enabled.")
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
            check_deployment_health()

    except ApiException as exc:
        print(f"Kubernetes API error: {exc}")

    except KeyboardInterrupt:
        print("\nStopping observer...")

    finally:
        watcher.stop()


if __name__ == "__main__":
    observe()