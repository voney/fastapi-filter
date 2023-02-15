# -*- coding: utf-8 -*-
from enum import Enum
from typing import List, Tuple, Union
from geoalchemy2.types import Geography, Geometry
from sqlalchemy.sql import cast
from pydantic import validator, confloat
from pydantic.fields import ModelField, SHAPE_TUPLE
from geoalchemy2 import functions
from sqlalchemy.orm import Query
from sqlalchemy.sql.selectable import Select
from sqlalchemy import or_
from fastapi_filter.contrib.sqlalchemy.filter import (
    Filter as SQLAlchemyFilter,
    _orm_operator_transformer as _sqla_orm_operator_transformer,
)

import logging

logger = logging.getLogger(__name__)

SRS_WGS_84 = 4326

BoundingBox = Tuple[
    confloat(ge=-90, allow_inf_nan=False),
    confloat(ge=-180, allow_inf_nan=False),
    confloat(le=90, allow_inf_nan=False),
    confloat(le=180, allow_inf_nan=False),
]


def bbox_to_geometry(bbox: List[float]):
    # ST_MakeEnvelope(float xmin, float ymin, float xmax, float ymax, integer srid=unknown);
    # SRID 4326 means we're speaking lat/long in the coordinate system.
    return functions.ST_MakeEnvelope(bbox[0], bbox[1], bbox[2], bbox[3])


_orm_operator_transformer = {
    **_sqla_orm_operator_transformer,
    "bbox": lambda value: ("contained", bbox_to_geometry(value)),
}


def find_type(class_, colname: str):
    if hasattr(class_, "__table__") and colname in class_.__table__.c:
        return type(class_.__table__.c[colname].type)
    for base in class_.__bases__:
        return find_type(base, colname)
    raise NameError(colname)


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
    def split_str(cls, value, field: ModelField):
        logger.info("Processing field: %s, value: %s", field, value)
        if (
            field.name == cls.Constants.ordering_field_name
            or field.name.endswith("__in")
            or field.name.endswith("__not_in")
            or field.name.endswith("__bbox")
        ) and isinstance(value, str):
            if field.shape == SHAPE_TUPLE:
                return (field.type_(v) for v in value.split(","))
            return [field.type_(v) for v in value.split(",")]
        return value

    def filter(self, query: Union[Query, Select]):
        for field_name, value in self.filtering_fields:
            field_value = getattr(self, field_name)
            if isinstance(field_value, Filter):
                query = field_value.filter(query)
            else:
                if "__" in field_name:
                    field_name, operator = field_name.split("__")
                    operator, value = _orm_operator_transformer[operator](value)
                else:
                    operator = "__eq__"

                if field_name == self.Constants.search_field_name and hasattr(self.Constants, "search_model_fields"):

                    def search_filter(field):
                        return getattr(self.Constants.model, field).ilike("%" + value + "%")

                    query = query.filter(or_(*list(map(search_filter, self.Constants.search_model_fields))))
                else:
                    model_field = getattr(self.Constants.model, field_name)
                    field_type = find_type(self.Constants.model, field_name)
                    # Geography types don't have all the operators we need, so cast them to Geometry
                    if field_type == Geography:
                        query = query.filter(getattr(cast(model_field, Geometry(srid=SRS_WGS_84)), operator)(value))
                    else:
                        query.filter(getattr(model_field), operator)(value)

        return query
