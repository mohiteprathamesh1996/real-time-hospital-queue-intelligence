from datetime import timedelta

import numpy as np

from simulator.event_models import HospitalEvent


class EventFaultInjector:
    """Inject controlled streaming data-quality faults."""

    def __init__(
        self,
        duplicate_rate: float,
        late_event_rate: float,
        out_of_order_rate: float,
        seed: int | None = None,
    ):
        if not 0 <= duplicate_rate <= 1:
            raise ValueError(
                "duplicate_rate must be between 0 and 1"
            )

        if not 0 <= late_event_rate <= 1:
            raise ValueError(
                "late_event_rate must be between 0 and 1"
            )

        if not 0 <= out_of_order_rate <= 1:
            raise ValueError(
                "out_of_order_rate must be between 0 and 1"
            )

        self.duplicate_rate = duplicate_rate
        self.late_event_rate = late_event_rate
        self.out_of_order_rate = out_of_order_rate

        self.rng = np.random.default_rng(seed)

    def inject(
        self,
        events: list[HospitalEvent],
    ) -> list[HospitalEvent]:
        """Return events with controlled streaming faults."""

        output: list[HospitalEvent] = []

        for event in events:

            # -------------------------------------------------------
            # Original event
            # -------------------------------------------------------

            output.append(event)

            # -------------------------------------------------------
            # Duplicate event
            # -------------------------------------------------------

            if self.rng.random() < self.duplicate_rate:
                output.append(event.model_copy(deep=True))

            # -------------------------------------------------------
            # Late event
            #
            # event_time is moved backwards while ingestion happens
            # normally when the event reaches the producer.
            # -------------------------------------------------------

            if self.rng.random() < self.late_event_rate:
                late_event = event.model_copy(deep=True)

                late_event.event_time = (
                    late_event.event_time
                    - timedelta(
                        minutes=float(
                            self.rng.uniform(5, 30)
                        )
                    )
                )

                output.append(late_event)

        # -----------------------------------------------------------
        # Out-of-order delivery
        #
        # We don't change event_time here. We change delivery order.
        # -----------------------------------------------------------

        if len(output) > 1:

            for index in range(len(output) - 1):

                if self.rng.random() < self.out_of_order_rate:

                    output[index], output[index + 1] = (
                        output[index + 1],
                        output[index],
                    )

        return output