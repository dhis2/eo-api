from typing import Protocol, TypeAlias

ParameterMap: TypeAlias = dict[str, dict]
BBox: TypeAlias = tuple[float, float, float, float]
Point: TypeAlias = tuple[float, float]
SourcePayload: TypeAlias = dict


class CoverageResolver(Protocol):
    def __call__(
        self,
        datetime_value: str,
        parameters: ParameterMap,
        bbox: BBox,
    ) -> SourcePayload: ...


class PositionResolver(Protocol):
    def __call__(
        self,
        datetime_value: str,
        parameters: ParameterMap,
        coords: Point,
    ) -> SourcePayload: ...


class AreaResolver(Protocol):
    def __call__(
        self,
        datetime_value: str,
        parameters: ParameterMap,
        bbox: BBox,
    ) -> SourcePayload: ...
