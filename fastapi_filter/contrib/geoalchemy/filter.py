# -*- coding: utf-8 -*-
from enum import Enum
from typing import List

from pydantic import validator
from geoalchemy2 import functions

from ..sqlalchemy.filter import Filter as SQLAlchemyFilter, _orm_operator_transformer as _sqla_orm_operator_transformer


def bbox_to_geometry(bbox: List[float]):
    # ST_MakeEnvelope(float xmin, float ymin, float xmax, float ymax, integer srid=unknown);
    # SRID 4326 means we're speaking lat/long in the coordinate system.
    functions.ST_MakeEnvelope(bbox[0], bbox[1], bbox[2], bbox[3], srid=4326)


_orm_operator_transformer = {
    **_sqla_orm_operator_transformer,
    "bbox": lambda value: ("contained", bbox_to_geometry(value)),
}


class Filter(SQLAlchemyFilter):
    """Base filter for geoalchemy related filters.

    All children must set:
        ```python
        class Constants(Filter.Constants):
            model = MyModel
        ```

    It can handle regular field names, Django style operators and geoalchemy operators.

    Example:
        ```python
        class MyModel:
            id: PrimaryKey()
            name: StringField(nullable=True)
            count: IntegerField()
            created_at: DatetimeField()

        class MyModelFilter(Filter):
            id: Optional[int]
            id__in: Optional[str]
            count: Optional[int]
            count__lte: Optional[int]
            created_at__gt: Optional[datetime]
            name__isnull: Optional[bool]
    """

    class Direction(str, Enum):
        asc = "asc"
        desc = "desc"

    @validator("*", pre=True)
    def split_str(cls, value, field):
        if (
            field.name == cls.Constants.ordering_field_name
            or field.name.endswith("__in")
            or field.name.endswith("__not_in")
            or field.name.endswith("__bbox")
        ) and isinstance(value, str):
            return [field.type_(v) for v in value.split(",")]
        return value
