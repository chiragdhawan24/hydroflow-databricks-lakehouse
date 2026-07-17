# Raw Data Contracts

## meter_readings JSONL

Each row represents one smart-meter event.

| Column | Type | Description |
|---|---|---|
| event_id | string | Unique event identifier; intentional duplicates may occur |
| source_system | string | Source system name |
| source_entity | string | Always `meter_readings` |
| event_timestamp | string | ISO-8601 event timestamp |
| meter_id | string | Meter identifier; may be null in invalid records |
| customer_id | string | Customer identifier |
| service_zone_id | string | GIS/service zone identifier |
| meter_type | string | water or electric |
| usage_value | double | Usage amount; may be negative in invalid records |
| usage_unit | string | gallons or kwh |
| pressure_psi | double | Water pressure for water meters |
| battery_pct | integer | Meter battery percentage |
| firmware_version | string | Firmware version |
| meter_status | string | active, maintenance, offline; invalid values may occur |
| is_intentionally_invalid | boolean | Indicates synthetic invalid records |

## outage_events JSONL

Each row represents an outage, leak, pressure drop, or interruption alert.

| Column | Type | Description |
|---|---|---|
| event_id | string | Unique event identifier |
| outage_id | string | Outage identifier |
| meter_id | string | Affected meter |
| customer_id | string | Affected customer |
| service_zone_id | string | Service zone |
| event_type | string | pressure_drop, suspected_leak, meter_offline, service_interruption |
| severity | string | low, medium, high, critical |
| event_timestamp | string | Start timestamp |
| resolved_timestamp | string | Resolution timestamp |

## customer_cdc JSONL

Each row represents a customer change event.

| Column | Type | Description |
|---|---|---|
| cdc_event_id | string | CDC event identifier |
| operation | string | insert, update, or delete |
| sequence_num | long | Ordering field for CDC processing |
| event_timestamp | string | Change timestamp |
| customer | struct | Customer payload after the operation |

## billing JSONL

Each row represents a monthly customer bill.

| Column | Type | Description |
|---|---|---|
| bill_id | string | Billing record identifier |
| customer_id | string | Customer identifier |
| service_zone_id | string | Service zone |
| billing_month | string | Month date |
| usage_gallons | double | Monthly usage |
| bill_amount_usd | double | Bill amount |
| payment_status | string | paid, late, unpaid |
| generated_timestamp | string | Billing file generation timestamp |

## gis_zones CSV

Static service-zone lookup data for stream-static joins.

| Column | Type | Description |
|---|---|---|
| zone_id | string | Service zone identifier |
| zone_name | string | Human-readable zone name |
| city | string | City |
| state | string | State |
| pressure_band | string | low, medium, high |
| service_priority | string | standard, critical, industrial |
| centroid_latitude | double | Synthetic latitude |
| centroid_longitude | double | Synthetic longitude |
