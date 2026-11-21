# 008: A new version becomes `challenger` only if its holdout MAE is at least 1% better, no slice gets much worse, and p95 stays within 50 ms

Stage: S2 · Date: 2026-11-21 · Author: Prince0906 · Drafted by agent: yes (written at the author's request; the numbers are the author's to confirm)

## Choice

`beats_champion` (blocking, on `eval_report`) compares the new version with the champion, or with the v1
baseline fitted on the same window before there is a champion. Both run on the same holdout rows. Every
rule must pass:

| Rule | Limit | Constant in `training.py` |
|---|---|---|
| Overall holdout MAE | at least **1%** below the reference's | `MAE_MARGIN = 0.01` |
| `airport`, `manhattan` slice MAE | at most **2%** worse than the reference's | `SLICE_TOLERANCE` |
| `outer`, `night` slice MAE | at most **5%** worse | `SLICE_TOLERANCE` |
| p95 single-row predict, in process | at most **50 ms** | `LATENCY_BUDGET_MS = 50` |
| Same holdout | both models scored on the same, non-empty rows | `judge` |

The `challenger` asset judges the report again before it moves the alias, and refuses if the champion
has changed since the report. A blocking check only stops steps in its own run. Without that second
check, materializing `challenger` on its own would reuse the last stored report.

## Rejected alternative

**Any improvement at all** (new MAE simply lower). On 3.5M holdout rows the MAE's own sampling error
is about 0.005 minutes, so "lower" can mean 0.3 seconds a trip. Every retrain would then move
`challenger`, and each move brings S3's canary and promotion work and their risk, for a gain no rider
notices.

I also rejected **RMSE as the metric**. A few trips take close to three hours (traffic, a wrong dropoff
time), and RMSE squares their errors. For the constant 12.02-minute guess on April, RMSE is 13.6 against
an MAE of 8.6, and most of that gap comes from those few trips. A model tuned for RMSE gets pulled
towards them and quotes worse times for the normal 10-minute ride. MAE is "how many minutes off a
quote usually is", which is what a rider feels. v2 is also trained on absolute error (`objective: l1`).

## Why

- **1% is a real difference, not noise.** For v1 on April it is about 0.04 minutes (3.956 × 1%), which
  is about 8 times the holdout's sampling error. Both models are scored on the same rows, so the
  month-to-month shift (v1 went from 3.901 on March to 3.956 on April, +1.4%) moves them together and
  does not need to fit inside the margin.
- **Slices stop a trade I don't want.** A model can improve the overall MAE through Manhattan (84.5% of
  April's rows) while getting worse at the airports (9.6% of rows, the longest and most visible
  quotes). `outer` (3.0%, about 100k rows) and `night` (7.9%) are smaller and moved the most between my
  two backtest folds (outer 9.64 → 9.19, night 3.14 → 2.87), so they get a wider tolerance. A tolerance
  that tight would block on noise.
- **The 50 ms budget comes from the app.** The app gives serving 300 ms and then falls back to the
  static median (`app/README.md`). The model gets a sixth of that. The rest goes to the network,
  Traefik, JSON and a cold request.
- Cost of this choice: a version that is truly 0.5% better is not promoted. I accept that. The next
  month's retrain will try again on fresher data.

## What would show this was wrong

- A version passes the gate, and S3's canary shows worse live error than the champion. Then the holdout
  is not a good stand-in for live traffic, and the margin is not the fix.
- The gate keeps failing for months while `drift_report` shows a real shift. Then the margin is too
  strict for how much a retrain can gain.
- p95 in serving goes over 300 ms, so the app falls back, while the in-process p95 stays under 50 ms.
  The budget is measured on the platform host, but serving runs on the app host. If they differ, the
  measurement should move to serving.
- A slice slowly gets worse over several promotions. Each promotion may cost a slice up to 2% to 5%,
  and that adds up. If it happens, compare slices with the first champion, not only the current one.
