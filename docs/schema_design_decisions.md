# V2 Star Schema — Design Decisions

**Author**: Sumit Kumar Prajapat
**Date**: 2026-05-09
**Branch**: v2-real-data
**Context**: Rebuilding the healthcare claims project on real Medicare data
(Kaggle Healthcare Provider Fraud Detection dataset, ~558K claims across
40K inpatient + 518K outpatient records). V1 used synthetic data with
clean 1:1 relationships; the real data has multi-valued attributes
(10 diagnosis codes, 6 procedure codes, 3 physician roles per claim)
that force schema choices V1 never had to make.

## Summary
- 2 fact tables (`fact_inpatient_claims`, `fact_outpatient_claims`)
- 5 dimension tables (`dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis`, `dim_procedure`)
- 2 bridge tables (`bridge_claim_diagnosis`, `bridge_claim_procedure`)
- 9 DDL files in `sql/schema/`

---

## Decision 1: Two separate fact tables for inpatient and outpatient

**Chosen**: Option A — `fact_inpatient_claims` and `fact_outpatient_claims`

**Why**:
- **Different grain.** One row in `fact_outpatient_claims` represents a single visit; one row in `fact_inpatient_claims` represents a multi-day admission with `AdmissionDt`/`DischargeDt`. Kimball is explicit that mixing grains in a single fact is a design error — every aggregation downstream has to remember which "type" of row it's looking at.
- **Avoiding always-null columns.** A unified fact would need `AdmissionDt`, `DischargeDt`, `DiagnosisGroupCode` (DRG), and `AdmitDiagnosisCode` as nullable columns. With 518K outpatient rows out of 558K total, those columns would be NULL ~93% of the time — a textbook smell that two distinct entities are being shoehorned into one table.
- **Different fraud and reimbursement patterns.** Inpatient fraud tends to be DRG upcoding and length-of-stay manipulation; outpatient fraud is unbundling and visit-volume inflation. Reimbursement is DRG-based for inpatient and fee-schedule for outpatient. Combining them dilutes signal in any model trained on the unified table and makes cost-structure analysis ambiguous.

**Trade-off accepted**:
- Combined reporting requires a `v_all_claims` view using `UNION ALL` over the columns common to both.
- Two tables to maintain instead of one — minor ongoing cost for clearly-bounded benefit.

**Interview defense**:
- "Different grain. An outpatient claim is one visit; an inpatient claim is one multi-day admission. They have different attributes (DRG, admit/discharge dates), different fraud patterns, and different reimbursement formulas. Forcing them into one fact table would mean ~93% of rows have NULL in the inpatient-only columns, which is the canonical sign that two facts are being collapsed into one."

---

## Decision 2: Bridge tables for diagnosis and procedure codes

**Chosen**: Option B — `bridge_claim_diagnosis` and `bridge_claim_procedure`

**Bridge structure**:
- `bridge_claim_diagnosis`: `claim_id`, `diagnosis_code` (FK → `dim_diagnosis`), `diagnosis_position` (1–10)
- `bridge_claim_procedure`: `claim_id`, `procedure_code` (FK → `dim_procedure`), `procedure_position` (1–6)
- Position 1 = primary (the principal reason for admission/visit); positions 2–10 = secondary (comorbidities and complications)

**Why**:
- **Filter queries become natural.** "Find all claims involving diabetes (ICD 250.x)" is one `WHERE diagnosis_code LIKE '250%'` in the bridge. With 10 wide columns, the same query is `WHERE ClmDiagnosisCode_1 LIKE '250%' OR ClmDiagnosisCode_2 LIKE '250%' OR ... OR ClmDiagnosisCode_10 LIKE '250%'` — verbose, slow, and brittle.
- **Aggregations are clean.** "Top 20 diagnoses driving cost" is a single `GROUP BY diagnosis_code` over the bridge joined to the fact. The 10-column version requires UNPIVOT or ten UNION ALL passes over the fact table.
- **Indexing is dramatically cheaper.** A single composite index on `(diagnosis_code, claim_id)` serves every code-filter query. The 10-column version needs 10 separate indexes (or accepts full scans on 9 of every 10 queries).
- **Industry standard.** ICD codes are modeled this way in CMS data warehouses, in OMOP-CDM, and in essentially every claims data product I've seen — because they're the canonical example of multi-valued claim attributes.
- **Why not drop secondary diagnoses (Option C):** comorbidities are clinically and analytically critical. Two patients with primary diagnosis "chest pain" cost very differently if one also has diabetes + CHF + CKD. Dropping positions 2–10 throws away exactly the signal that distinguishes legitimately complex cases from upcoded simple ones — a key fraud feature.

**Trade-off accepted**:
- `bridge_claim_diagnosis` will hit ~2.5–3M rows (558K claims × ~4–5 avg non-null codes). Bigger than the fact tables themselves.
- Needs a composite index `(diagnosis_code, claim_id)` for filtering and `(claim_id, diagnosis_position)` for joining back to fact.
- Schema is one step further from "what the source CSV looks like" — onboarding cost mitigated by this doc.

---

## Decision 3: Three FK columns for physician roles

**Chosen**: Option A — `attending_physician_id`, `operating_physician_id`, `other_physician_id` as three FK columns directly on the fact table

**Why**:
- **Cardinality is fixed at 3, not variable.** Bridge tables justify themselves when the count of related entities varies per parent (1–10 diagnoses, 0–6 procedures). The source has exactly three physician slots, always — making this closer to "billing address vs shipping address" than to "claim diagnoses".
- **Each role has distinct semantic meaning.** Attending = the physician with clinical responsibility for the case; Operating = the surgeon performing the procedure; Other = consults and supporting clinicians. With three named columns, "find providers whose attending physician also operated" is a clean self-join. With a bridge, every physician query has to add `WHERE role = 'X'` filters that recover the columnar structure we just normalized away.
- **Bridge would over-engineer for fixed cardinality.** Three columns × one row beats one bridge table × three rows × an extra `role` enum in storage, query complexity, and conceptual load.
- **Why not drop the others (Option C):** "Other physician" patterns are a real fraud signal — providers who repeatedly appear as "Other" on high-value inpatient claims (often signaling kickback referral chains) are flagged by exactly this column. Dropping it discards a feature.

**Trade-off accepted**:
- If CMS ever adds a 4th physician role, schema change required. Low probability — the 3-role structure has been stable in CMS claim files for decades.
- Three joins to a future `dim_physician` instead of one. Mitigated by the fact that few queries need all three roles simultaneously, and indexed FK joins are cheap.

---

## Open questions (to revisit later)

- **Should physician IDs become a `dim_physician` table eventually?**
  Currently keeping as plain `VARCHAR` FKs without a dim, because the source has only IDs — no physician attributes (name, specialty, credentialing date) to populate a dim with. Revisit on Day 9 if the ML model needs physician-level features (e.g., aggregate prior fraud rate per physician).

- **Do we need an SCD strategy for `dim_provider.is_potentially_fraudulent`?**
  No. The fraud label is fixed in the source data — it's a one-time labeling, not a state that evolves. Type 1 dimension (overwrite on update) is sufficient. If we later add provider attributes that DO change over time (NPI registration status, sanction history), Type 2 may be warranted for those columns specifically.

- **Should `dim_diagnosis` include ICD-9 vs ICD-10 awareness?**
  This dataset uses ICD-9 (the cutover to ICD-10 was Oct 2015 in the US, and the data appears to predate it). For now, treat all codes as ICD-9. If real-world deployment needs to span the cutover, add a `code_system` column to `dim_diagnosis` and the bridge.
