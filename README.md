Wistia Video Analytics: End-to-End AWS Data Pipeline
🚀 Overview
This project demonstrates a robust, cloud-native data engineering pipeline built entirely on AWS. The pipeline ingests, processes, and analyzes video engagement and visitor data from the Wistia Stats API, enabling data-driven marketing insights for videos distributed on Facebook and YouTube.

📐 Architecture
Technologies Used:

AWS Lambda (API ingestion in Python)

Amazon S3 (raw and processed data storage)

AWS Glue (ETL & transformations, PySpark)

AWS Athena (ad hoc analytics/SQL reporting)

AWS Secrets Manager (secure API key management)

AWS IAM (security)

GitHub Actions (CI/CD)

<p align="center"> <img src="https://user-images.githubusercontent.com/11109519/276601883-817d90c2-2e4b-4c3b-8cce-91e0136b3602.png" width="640" alt="Wistia AWS Data Pipeline Architecture"> </p>
🛠️ Pipeline Flow
Ingestion (Lambda):

Authenticates with Wistia API using token from Secrets Manager.

Collects:

Media stats (/stats/medias/{media_id}.json)

Media metadata (/medias/{media_id}.json)

Media engagement stats (/stats/medias/{media_id}/engagement.json)

Visitor-level events (/stats/events.json)

Stores all data as JSON in Amazon S3 (raw zone).

ETL (Glue):

dim_media: Latest, deduplicated stats and metadata per video.

dim_visitor: Unique visitors, with IP and country (from events).

fact_media_engagement: Aggregates visitor/video/date engagement with media-level metrics.

Analytics (Athena):

Athena tables over S3 processed zone.

Ad hoc queries, data validation, and support for reporting.

CI/CD (GitHub Actions):

Linting and code quality checks for Lambda and Glue scripts.

Optional auto-deployment to AWS Lambda.

🗂️ Repo Structure
bash
Copy code
wistia-analytics-pipeline/
│
├── lambda/                        # Python Lambda ingestion code
├── glue/                          # Glue ETL PySpark scripts
├── .github/workflows/             # CI/CD with GitHub Actions
├── README.md
└── requirements.txt
💡 Sample Analytics
Table: fact_media_engagement

media_id	visitor_id	date	play_count	watched_percent_event
ayxhuq5wcn	...	2025-06-18	2	0.28

Example Athena query:

sql
Copy code
SELECT
  media_id,
  COUNT(DISTINCT visitor_id) AS unique_viewers,
  AVG(watched_percent_event) AS avg_percent_viewed
FROM wistia_analytics.fact_media_engagement
GROUP BY media_id;
📝 Setup & Deployment
Clone this repo

Edit and push Lambda/Glue code locally

Deploy Lambda via AWS Console/CLI

Deploy Glue jobs (upload scripts to S3 and configure)

Register Athena tables for analysis

Run and schedule pipeline with EventBridge (daily or as needed)

See lambda/, glue/, and .github/workflows/ for code and automation details.

🛡️ Security & Credentials
All sensitive keys/secrets are managed in AWS Secrets Manager.

IAM roles are least-privilege, scoped to Lambda and Glue job requirements.

No credentials are stored in code or version control.

📈 Business Value
Enables marketers and analysts to track video and visitor engagement across platforms.

Supports data-driven optimization of video content strategy.

Scalable, cost-effective, and maintainable cloud-native solution.

🧑‍💻 Author
Garrett Moore
LinkedIn (update this link!)

📣 Questions?
Open an issue or contact the author directly.