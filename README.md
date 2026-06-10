# ClearPath Primary Care — 30-Day Hospital Readmission Pipeline

An end-to-end big data pipeline that predicts 30-day hospital readmissions, surfaces care-management KPIs, and raises real-time patient-vitals alerts — built on Databricks with a Medallion architecture on AWS S3 and a Power BI front end.

> Built for a graduate Big Data Analytics course (ISM-6362). The scenario is a fictional primary care network, "ClearPath Primary Care," that wants to reduce costly 30-day readmissions across ~70,000 chronic-disease patients.

---

## Tech Stack

`Databricks` · `Apache Spark (PySpark 4.0)` · `Spark Structured Streaming` · `Spark ML` · `AWS S3` · `Power BI` · `Medallion Architecture`

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [How It Works](#how-it-works)
- [Data Cleaning](#data-cleaning)
- [KPIs](#kpis)
- [Machine Learning Model](#machine-learning-model)
- [Dashboard](#dashboard)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Future Improvements](#future-improvements)
- [Acknowledgments](#acknowledgments)

---

## Overview

When a patient is readmitted within 30 days of discharge, the clinic faces Medicare penalties and a sign that post-discharge care fell short. With a small care-management team and tens of thousands of patients, manual chart review isn't feasible. This project uses the clinic's data three ways:

1. **Streaming** — real-time vitals alerting so care managers can act on deterioration as it happens.
2. **Batch** — nightly KPIs that show where readmissions are concentrating.
3. **Machine Learning** — a per-patient readmission risk score to target outreach.

---

## Architecture

The pipeline follows a **Medallion (Bronze → Silver → Gold)** architecture on Amazon S3, with Spark on Databricks for processing and Power BI for presentation. Two parallel paths — a real-time "hot" path for vitals and a historical "cold" path for analytics — converge only at the business level.

![Architecture Diagram](assets/architecture.png)

| Layer | Contents |
| --- | --- |
| **Bronze (Transactional)** | Raw streamed vitals events |
| **Bronze (Dimensional)** | Raw historical encounter data |
| **Silver (Cleaned)** | Cleaned, de-duplicated, labeled encounters |
| **Gold (Refined)** | Aggregated KPI tables |
| **Gold (Inferences)** | Per-patient model risk scores |

---

## Dataset

[Diabetes 130-US Hospitals (1999–2008)](https://www.kaggle.com/datasets/brandao/diabetes) — 101,766 diabetic patient encounters with demographics, diagnoses, prior-visit counts, lab results, and a 30-day readmission label. Diabetes is managed primarily in primary care, which fits the ClearPath scenario.

The streaming use case uses **separately generated synthetic vitals events** (timestamped heart rate, blood pressure, SpO₂, temperature), kept distinct from the encounter data because the two have different shapes.

---

## How It Works

**Streaming path.** Synthetic vitals events land as JSON in S3 Bronze (Transactional). A Spark Structured Streaming query reads the stream, groups events into **5-minute tumbling windows per patient with a 10-minute watermark**, and flags threshold breaches as `TACHYCARDIA`, `HYPERTENSIVE`, `LOW_OXYGEN`, or `FEVER`.

**Batch path.** Encounter data loads from S3 Bronze (Dimensional), is cleaned, and is written to S3 Silver. From Silver, a batch job computes three KPIs into S3 Gold (Refined), and an ML job writes per-patient risk scores into S3 Gold (Inferences). Power BI reads the Gold layer.

---

## Data Cleaning

Raw `diabetic_data.csv` (101,766 rows) was reduced to **69,987 analysis-ready encounters** through a PySpark workflow:

- Converted `?` placeholders to true nulls on string columns.
- Dropped sparse columns (weight ~97% missing, payer code ~40% missing) and near-constant medication columns.
- Removed invalid-gender rows and encounters ending in death or hospice discharge (a patient who died can't be readmitted, which would corrupt the label).
- De-duplicated to one (earliest) encounter per patient to prevent train/test leakage.
- Derived the binary `readmit_30d` target plus `diagnosis_group`, `age_group`, and `admission_type` groupings.

The resulting count (69,987) matches the published baseline (Strack et al., 2014), and the 30-day readmission rate is ~9%.

---

## KPIs

Reported on a nightly batch cadence for care managers:

| KPI | Finding |
| --- | --- |
| **Readmission rate by diagnosis group** | Injury (10.8%) and Circulatory (9.7%) highest; Respiratory lowest (7.3%) |
| **Avg length of stay by admission type** | Trauma Center longest (5.4 days), Newborn shortest (3.2 days) |
| **Readmission rate by prior utilization** | 8% (0 prior) → 14% (1–2 prior) → 26% (3+ prior) |

The clearest operational signal: a patient's prior-admission history roughly triples their readmission risk.

---

## Machine Learning Model

- **Goal:** predict 30-day readmission per patient.
- **Model:** Random Forest (Spark ML Pipeline) with a Logistic Regression baseline.
- **Features:** age group, diagnosis group, admission type, A1C result, length of stay, medication/lab counts, and prior inpatient/emergency/outpatient visit counts.
- **Split:** 80/20 train/test (~56K / ~14K).
- **Metric:** **AUC-ROC** (~0.60), chosen over accuracy because only ~9% of patients are readmitted — a "predict no" model would score ~91% accuracy while being useless.
- **Top drivers:** diagnosis group and prior hospital utilization, consistent with the batch KPIs.

The value is **prioritization, not perfect prediction** — scoring the whole population is cheap and automated, and reliably surfacing the highest-risk cohort is far more cost-effective than manual review.

---

## Dashboard

An interactive Power BI dashboard reads the Gold layer and presents:

- Headline cards: total patients, 30-day readmissions, average length of stay.
- The three KPI charts.
- A patient-volume-by-diagnosis donut (volume vs. rate contrast).
- The ML predicted-risk distribution.

![Dashboard](assets/dashboard.png)

---

## Repository Structure

```
.
├── notebooks/
│   └── ClearPath_Pipeline.ipynb     # Databricks notebook: cleaning, KPIs, ML, streaming
├── dashboard/
│   └── ClearPath_Dashboard.pbix     # Power BI dashboard
├── diagrams/
│   └── ClearPath_Architecture.drawio
├── docs/
│   └── ClearPath_Report.pdf         # Project report
├── assets/
│   ├── architecture.png
│   └── dashboard.png
└── README.md
```

---

## Getting Started

This pipeline runs on **Databricks** (built on Free Edition, serverless / Spark Connect) with data stored in **Amazon S3**.

1. Create an S3 bucket and note the base path (e.g., `s3://your-bucket/PrimaryCare_Data`).
2. Upload the diabetes dataset to the Bronze (Dimensional) location.
3. Open `notebooks/ClearPath_Pipeline.ipynb` in Databricks and set `Data_Path` to your S3 base path.
4. Run the notebook top to bottom: cleaning → Silver, KPIs → Gold/Refined, ML → Gold/Inferences, and the streaming alert query.
5. Connect Power BI to the Gold-layer outputs to load the dashboard.

> **Note:** On serverless workspaces, `.cache()` isn't supported and sessions reset, so the notebook reloads cleaned data from S3 Silver between stages. Structured Streaming requires an explicit `checkpointLocation`.

---

## Future Improvements

- Wire streaming alerts to real notification delivery (e.g., a messaging or paging service).
- Address class imbalance with class weighting or resampling to improve recall on readmitted patients.
- Add more clinical features and tune the model.
- Replace the synthetic stream with a live ingestion source (Kinesis / Kafka).

---

## Acknowledgments

- Dataset: [Diabetes 130-US Hospitals (1999–2008)](https://www.kaggle.com/datasets/brandao/diabetes), based on Strack et al. (2014).
- Built as a graduate Big Data Analytics final project.
