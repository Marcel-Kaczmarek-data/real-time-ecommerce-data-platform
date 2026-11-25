"""Single source of truth for event contracts.

Payload field definitions are declared once here. From them we derive:
  * Avro schemas (``schemas/avro/<entity>.avsc``): what producers register in Schema Registry
  * JSON Schemas (``schemas/json/<entity>.schema.json``): for non-Avro consumers / docs
  * Spark DDL used by Silver to parse ``raw_payload``

Run ``python -m schemas.contracts`` to regenerate the files; a unit test fails
if the committed files drift from these definitions.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCHEMA_DIR = Path(__file__).resolve().parent
NAMESPACE = "com.ecommerce.events"

# (name, type, nullable, doc). Types: string | int | long | double | boolean | timestamp
Field = tuple[str, str, bool, str]

PAYLOADS: dict[str, list[Field]] = {
    "customers": [
        ("customer_id", "string", False, "Business key, CUST-nnnnnnn"),
        ("first_name", "string", False, "PII - masked in Silver"),
        ("last_name", "string", False, "PII - masked in Silver"),
        ("email", "string", False, "PII - hashed/masked in Silver"),
        ("country", "string", False, "ISO 3166-1 alpha-2"),
        ("registration_date", "timestamp", False, "UTC"),
        ("customer_segment", "string", False, "consumer | small_business | corporate"),
        ("loyalty_status", "string", False, "none | bronze | silver | gold | platinum"),
        ("marketing_opt_in", "boolean", False, "GDPR marketing consent"),
        ("updated_at", "timestamp", False, "Last change in the source system (UTC)"),
    ],
    "orders": [
        ("order_id", "string", False, "Business key"),
        ("customer_id", "string", False, "FK customers"),
        ("order_timestamp", "timestamp", False, "Order placement time (UTC)"),
        ("order_status", "string", False, "created | confirmed | cancelled | shipped | delivered | returned"),
        ("status_reason", "string", True, "Reason for cancellation"),
        ("sales_channel", "string", False, "web | mobile_app | marketplace | social_commerce"),
        ("currency", "string", False, "ISO 4217"),
        ("subtotal_amount", "double", False, "Sum of quantity * unit_price"),
        ("discount_amount", "double", False, "Promotion discount"),
        ("shipping_amount", "double", False, "Shipping fee"),
        ("tax_amount", "double", False, "VAT / sales tax"),
        ("total_amount", "double", False, "subtotal - discount + shipping + tax"),
        ("promotion_id", "string", True, "Applied promotion"),
        ("country", "string", False, "Ship-to country"),
        ("session_id", "string", True, "Web/app session that produced the order"),
        ("item_count", "int", False, "Units in the order"),
    ],
    "order_items": [
        ("order_item_id", "string", False, "Business key"),
        ("order_id", "string", False, "FK orders"),
        ("product_id", "string", False, "FK products"),
        ("quantity", "int", False, "Units"),
        ("unit_price", "double", False, "Localised unit price"),
        ("discount_amount", "double", False, "Line discount"),
        ("currency", "string", False, "ISO 4217"),
        ("line_number", "int", False, "1-based line number"),
    ],
    "payments": [
        ("payment_id", "string", False, "Business key (one per attempt)"),
        ("order_id", "string", False, "FK orders"),
        ("payment_method", "string", False, "credit_card | debit_card | paypal | ..."),
        ("payment_status", "string", False, "authorized | captured | failed"),
        ("payment_amount", "double", False, "Amount in order currency"),
        ("currency", "string", False, "ISO 4217"),
        ("payment_timestamp", "timestamp", False, "UTC"),
        ("attempt_number", "int", False, "1 = first attempt"),
        ("failure_reason", "string", True, "Gateway decline reason"),
        ("provider", "string", False, "Payment service provider"),
    ],
    "inventory": [
        ("movement_id", "string", False, "Business key"),
        ("warehouse_id", "string", False, "FK warehouses"),
        ("product_id", "string", False, "FK products"),
        ("movement_type", "string", False, "reserve | release | ship | replenish | return_restock | adjust"),
        ("quantity_delta_available", "int", False, "Change of available units"),
        ("quantity_delta_reserved", "int", False, "Change of reserved units"),
        ("available_quantity", "int", False, "Available units after the movement (informational)"),
        ("reserved_quantity", "int", False, "Reserved units after the movement (informational)"),
        ("reorder_threshold", "int", False, "Reorder point"),
        ("reference_id", "string", False, "Order / delivery / return / cycle-count id"),
        ("updated_at", "timestamp", False, "UTC"),
    ],
    "shipments": [
        ("shipment_id", "string", False, "Business key"),
        ("order_id", "string", False, "FK orders"),
        ("warehouse_id", "string", False, "Shipping warehouse"),
        ("carrier", "string", False, "Carrier name"),
        ("shipment_status", "string", False, "created | in_transit | delayed | delivered"),
        ("shipped_at", "timestamp", True, "Hand-over to carrier (UTC)"),
        ("estimated_delivery", "timestamp", False, "Promised delivery (UTC)"),
        ("actual_delivery", "timestamp", True, "Proof of delivery (UTC)"),
        ("tracking_number", "string", False, "Carrier tracking number"),
        ("destination_country", "string", False, "ISO 3166-1 alpha-2"),
    ],
    "clickstream": [
        ("customer_id", "string", True, "Known customer (logged in)"),
        ("anonymous_id", "string", False, "Cookie / device id"),
        ("session_id", "string", False, "Session id"),
        (
            "event_type",
            "string",
            False,
            "page_view | search | product_view | add_to_cart | remove_from_cart | checkout_started | purchase",
        ),
        ("product_id", "string", True, "Product context"),
        ("event_timestamp", "timestamp", False, "Client event time (UTC)"),
        ("device_type", "string", False, "mobile | desktop | tablet"),
        ("browser", "string", False, "Browser family"),
        ("traffic_source", "string", False, "Acquisition channel"),
        ("country", "string", False, "Geo-IP country"),
        ("page_url", "string", False, "Relative URL"),
        ("search_query", "string", True, "Search terms"),
        ("cart_quantity", "int", True, "Quantity added/removed"),
        ("campaign_id", "string", True, "Paid campaign"),
        ("sales_channel", "string", False, "web | mobile_app | social_commerce"),
    ],
    "refunds": [
        ("refund_id", "string", False, "Business key"),
        ("order_id", "string", False, "FK orders"),
        ("payment_id", "string", False, "Refunded payment"),
        ("return_id", "string", True, "Related return"),
        ("refund_amount", "double", False, "Gross refund incl. tax"),
        ("refund_tax_amount", "double", False, "Tax portion of the refund"),
        ("currency", "string", False, "ISO 4217"),
        ("refund_reason", "string", False, "order_cancelled | product_return"),
        ("refund_status", "string", False, "completed"),
        ("refund_timestamp", "timestamp", False, "UTC"),
    ],
    "returns": [
        ("return_id", "string", False, "Business key"),
        ("order_id", "string", False, "FK orders"),
        ("order_item_id", "string", False, "FK order items"),
        ("product_id", "string", False, "FK products"),
        ("quantity", "int", False, "Returned units"),
        ("return_reason", "string", False, "Customer stated reason"),
        ("return_status", "string", False, "requested | received"),
        ("requested_at", "timestamp", False, "UTC"),
        ("received_at", "timestamp", True, "UTC"),
        ("warehouse_id", "string", False, "Receiving warehouse"),
    ],
}

EVENT_TYPES: dict[str, list[str]] = {
    "customers": ["customer_registered", "customer_profile_updated"],
    "orders": [
        "order_created",
        "order_confirmed",
        "order_cancelled",
        "order_shipped",
        "order_delivered",
        "order_returned",
    ],
    "order_items": ["order_item_added"],
    "payments": ["payment_authorized", "payment_captured", "payment_failed"],
    "inventory": [
        "inventory_reserved",
        "inventory_released",
        "inventory_shipped",
        "inventory_replenished",
        "inventory_adjusted",
    ],
    "shipments": ["shipment_created", "shipment_in_transit", "shipment_delayed", "shipment_delivered"],
    "clickstream": [
        "page_view",
        "search",
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "purchase",
    ],
    "refunds": ["refund_issued"],
    "returns": ["return_requested", "return_received"],
}

_AVRO_TYPES = {
    "string": "string",
    "int": "int",
    "long": "long",
    "double": "double",
    "boolean": "boolean",
    "timestamp": {"type": "long", "logicalType": "timestamp-millis"},
}
_SPARK_TYPES = {
    "string": "STRING",
    "int": "INT",
    "long": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}
_JSON_TYPES = {
    "string": {"type": "string"},
    "int": {"type": "integer"},
    "long": {"type": "integer"},
    "double": {"type": "number"},
    "boolean": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
}


def _record_name(entity: str) -> str:
    return "".join(part.capitalize() for part in entity.split("_"))


def avro_schema(entity: str) -> dict[str, Any]:
    fields = []
    for name, ftype, nullable, doc in PAYLOADS[entity]:
        avro_type: Any = _AVRO_TYPES[ftype]
        field: dict[str, Any] = {"name": name, "doc": doc}
        if nullable:
            field["type"] = ["null", avro_type]
            field["default"] = None
        else:
            field["type"] = avro_type
        fields.append(field)
    record = _record_name(entity)
    return {
        "type": "record",
        "name": f"{record}Event",
        "namespace": NAMESPACE,
        "doc": f"Standard event envelope carrying a {entity} payload. Allowed event_type values: "
        + ", ".join(EVENT_TYPES[entity]),
        "fields": [
            {"name": "event_id", "type": "string", "doc": "Globally unique event id (UUID)"},
            {"name": "event_type", "type": "string", "doc": "Business event name"},
            {"name": "event_version", "type": "int", "doc": "Payload contract version"},
            {
                "name": "event_timestamp",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
                "doc": "Business event time (UTC)",
            },
            {"name": "source_system", "type": "string", "doc": "Emitting system"},
            {"name": "correlation_id", "type": "string", "doc": "Journey / order correlation id"},
            {"name": "payload", "type": {"type": "record", "name": f"{record}Payload", "fields": fields}},
        ],
    }


def json_schema(entity: str) -> dict[str, Any]:
    props: dict[str, Any] = {}
    required = []
    for name, ftype, nullable, doc in PAYLOADS[entity]:
        spec: dict[str, Any] = dict(_JSON_TYPES[ftype])
        if nullable:
            spec = {"anyOf": [spec, {"type": "null"}]}
        else:
            required.append(name)
        spec["description"] = doc
        props[name] = spec
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://schemas.ecommerce.example/{entity}.schema.json",
        "title": f"{_record_name(entity)}Event",
        "type": "object",
        "required": [
            "event_id",
            "event_type",
            "event_version",
            "event_timestamp",
            "source_system",
            "correlation_id",
            "payload",
        ],
        "properties": {
            "event_id": {"type": "string", "format": "uuid"},
            "event_type": {"enum": EVENT_TYPES[entity]},
            "event_version": {"type": "integer", "minimum": 1},
            "event_timestamp": {"type": "string", "format": "date-time"},
            "source_system": {"type": "string"},
            "correlation_id": {"type": "string"},
            "payload": {
                "type": "object",
                "required": required,
                "properties": props,
                "additionalProperties": False,
            },
        },
        "additionalProperties": False,
    }


def spark_ddl(entity: str) -> str:
    """Spark DDL string for parsing the JSON payload in Silver."""
    return ", ".join(f"{name} {_SPARK_TYPES[ftype]}" for name, ftype, _, _ in PAYLOADS[entity])


def required_fields(entity: str) -> list[str]:
    return [name for name, _, nullable, _ in PAYLOADS[entity] if not nullable]


def render_all() -> dict[Path, str]:
    out: dict[Path, str] = {}
    for entity in PAYLOADS:
        out[SCHEMA_DIR / "avro" / f"{entity}.avsc"] = json.dumps(avro_schema(entity), indent=2) + "\n"
        out[SCHEMA_DIR / "json" / f"{entity}.schema.json"] = json.dumps(json_schema(entity), indent=2) + "\n"
    return out


def main() -> None:
    for path, content in render_all().items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"wrote {path.relative_to(SCHEMA_DIR.parent)}")


if __name__ == "__main__":
    main()
