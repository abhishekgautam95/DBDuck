from importlib.metadata import PackageNotFoundError, version

from .UDOM import UDOM
from .models import (
    Boolean,
    BooleanField,
    CASCADE,
    CharField,
    Column,
    DO_NOTHING,
    DateTime,
    DateTimeField,
    Float,
    FloatField,
    ForeignKey,
    Integer,
    IntegerField,
    JSON,
    JSONField,
    ManyToMany,
    ManyToOne,
    OneToMany,
    OneToOne,
    RESTRICT,
    SET_NULL,
    String,
    TextField,
    UModel,
)

try:
    __version__ = version("DBDuck")
except PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = [
    "UDOM",
    "UModel",
    "Column",
    "String",
    "CharField",
    "TextField",
    "Integer",
    "IntegerField",
    "Float",
    "FloatField",
    "Boolean",
    "BooleanField",
    "JSON",
    "JSONField",
    "DateTime",
    "DateTimeField",
    "ForeignKey",
    "ManyToOne",
    "OneToOne",
    "OneToMany",
    "ManyToMany",
    "CASCADE",
    "SET_NULL",
    "RESTRICT",
    "DO_NOTHING",
    "__version__",
]
