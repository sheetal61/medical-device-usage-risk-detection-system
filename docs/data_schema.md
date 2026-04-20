# Device Log Data Schema

Each row represents one device activity event.

| Column        | Meaning |
|--------------|--------|
| device_id | Unique machine ID |
| device_type | Type of device (Ventilator, Pump, Monitor) |
| timestamp | When event happened |
| location | ICU, Ward, or OT |
| usage_minutes | How long device was used |
| cycles | Number of operations |
| status | ACTIVE, IDLE, or ERROR |
