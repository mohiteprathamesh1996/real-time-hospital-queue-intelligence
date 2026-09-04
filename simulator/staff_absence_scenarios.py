from dataclasses import dataclass


@dataclass
class StaffAvailabilityScenario:
    name: str
    planned_staff: int
    unavailable_staff: int
    available_staff: int


class StaffAbsenceScenarioGenerator:
    def generate(
        self,
        planned_staff: int,
        unavailable_staff: int,
    ) -> StaffAvailabilityScenario:
        if planned_staff <= 0:
            raise ValueError("planned_staff must be greater than 0")

        if unavailable_staff < 0:
            raise ValueError("unavailable_staff cannot be negative")

        if unavailable_staff >= planned_staff:
            raise ValueError(
                "unavailable_staff must be less than planned_staff"
            )

        return StaffAvailabilityScenario(
            name=f"{unavailable_staff} staff absent",
            planned_staff=planned_staff,
            unavailable_staff=unavailable_staff,
            available_staff=planned_staff - unavailable_staff,
        )