# Data Quality Report - Silver ecommerce_events

## Row counts
| Stage | Rows |
|---|---|
| Raw (bronze) | 109,950,743 |
| Rejected | 256,761 |
| Duplicates removed | 130,646 |
| Silver | 109,563,336 |

## Rejected rows by reason
| reject_reason | Rows |
|---|---|
| bad_price | 256761 

## Checks on silver
| Check | Result |
|---|---|
| Nulls in key columns | user_session = 12 |
| Event types | purchase 1659703, view 104079641, cart 3823992 |
| Price range | 0.77 - 2574.07 |
| Date range | 2019-10-01 - 2019-11-30 |
| Duplicate keys | 0 |

## Notes
Nulls in user user_session percentage is small but we should look at that case.