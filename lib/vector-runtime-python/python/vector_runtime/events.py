"""Optional dataclass adapter; plain dictionaries need no decorator."""
from dataclasses import dataclass, fields, is_dataclass


def external_event(cls):
    """Make a dataclass directly accepted by emit(), without asdict/deepcopy.

    Use field(metadata={"external_name": "process_id"}) to rename an output
    field. Native conversion validates actual values, not Python annotations.
    """
    if not isinstance(cls, type):
        raise TypeError("@external_event must decorate a class")
    if not is_dataclass(cls):
        cls = dataclass(cls)
    elif "__dataclass_fields__" not in cls.__dict__:
        # A subclass merely inherits dataclass markers; process its own fields.
        cls = dataclass(cls)
    layout = tuple(
        (field.name, field.metadata.get("external_name", field.name))
        for field in fields(cls)
    )
    names = [name for _, name in layout]
    if any(not isinstance(name, str) for name in names):
        raise TypeError("external_name must be a string")
    if len(set(names)) != len(names):
        raise ValueError("duplicate external event field name")
    cls.__vector_event_fields__ = layout
    return cls
