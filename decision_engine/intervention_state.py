from dataclasses import dataclass
from datetime import datetime


@dataclass
class InterventionState:
    active_additional_staff: int = 0

    deployed_at: datetime | None = None
    last_updated_at: datetime | None = None

    hold_until: datetime | None = None

    def is_active(self) -> bool:
        return self.active_additional_staff > 0