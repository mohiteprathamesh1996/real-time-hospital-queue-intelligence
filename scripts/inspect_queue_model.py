from simulator.queue_model import MMcQueueModel


def main():
    print("=" * 60)
    print("M/M/c QUEUE MODEL INSPECTION")
    print("=" * 60)

    arrival_rate = 30
    service_rate = 10
    server_count = 4

    model = MMcQueueModel(
        arrival_rate_per_hour=arrival_rate,
        service_rate_per_hour=service_rate,
        server_count=server_count,
    )

    metrics = model.calculate()

    print()
    print("Inputs")
    print(f"  Arrival rate:        {metrics.arrival_rate_per_hour:.2f} patients/hour")
    print(f"  Service rate:        {metrics.service_rate_per_hour:.2f} patients/hour/staff")
    print(f"  Staff:               {metrics.server_count}")
    print()

    print("Queue Metrics")
    print(f"  Utilization:         {metrics.utilization:.2%}")
    print(f"  Probability wait:    {metrics.probability_wait:.2%}")
    print(f"  Average queue:       {metrics.average_queue_length:.2f} patients")
    print(f"  Average wait:        {metrics.average_wait_minutes:.2f} minutes")
    print()

    print("System Metrics")
    print(f"  Average system size: {metrics.average_system_length:.2f} patients")
    print(f"  Average system time: {metrics.average_system_time_minutes:.2f} minutes")


if __name__ == "__main__":
    main()