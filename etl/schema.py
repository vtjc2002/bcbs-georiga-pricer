import pyarrow as pa


def schema(*fields):
    return pa.schema([(name, kind) for name, kind in fields])


TEXT = pa.string()
STRINGS = pa.list_(TEXT)
SCHEMAS = {
    "groups": schema(("ref_key", TEXT), ("group_id", TEXT), ("networks", STRINGS)),
    "members": schema(("ref_key", TEXT), ("subgroup_key", TEXT), ("npi", TEXT)),
    "organizations": schema(
        ("subgroup_key", TEXT), ("tin_type", TEXT), ("tin_value", TEXT),
        ("business_name", TEXT),
    ),
    "services": schema(
        ("service_key", TEXT), ("code_type", TEXT), ("code_version", TEXT),
        ("code", TEXT), ("name", TEXT), ("description", TEXT),
        ("arrangement", TEXT), ("bundled_json", TEXT), ("covered_json", TEXT),
    ),
    "sets": schema(("set_key", TEXT), ("service_key", TEXT)),
    "set_groups": schema(("set_key", TEXT), ("group_id", TEXT)),
    "prices": schema(
        ("price_key", TEXT), ("set_key", TEXT),
        ("amount", pa.decimal128(28, 8)), ("price_type", TEXT),
        ("expiration", TEXT), ("service_codes", STRINGS),
        ("billing_class", TEXT), ("setting", TEXT), ("modifiers", STRINGS),
        ("additional_information", TEXT),
    ),
}
