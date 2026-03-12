from pydantic import BaseModel, ConfigDict

from ...schemas import PeriodType


class Dhis2DataValueSetConfig(BaseModel):
    """Mapping from aggregate outputs to DHIS2 DataValueSet fields."""

    data_element_uid: str
    category_option_combo_uid: str = "HllvX50cXC0"
    attribute_option_combo_uid: str = "HllvX50cXC0"
    data_set_uid: str | None = None
    org_unit_property: str = "id"
    stored_by: str | None = None


class _BuildDataValueSetStepConfig(BaseModel):
    # from workflows folder
    model_config = ConfigDict(extra="forbid")

    period_type: PeriodType | None = None
