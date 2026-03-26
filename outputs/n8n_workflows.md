# n8n Workflow Documentation — Medicare Physician Analytics

## Overview
Three n8n automation workflows that orchestrate data refresh, alerting,
and on-demand provider lookups. All workflows are designed for n8n.cloud
free tier or self-hosted n8n.

---

## Setup: n8n.cloud Free Tier

1. Sign up at https://app.n8n.cloud
2. Create a new workflow for each of the three workflows below
3. Install required community nodes (if self-hosted):
   - n8n-nodes-sqlite (or use the built-in SQLite node in v1.x)
4. Configure credentials:
   - **Email**: SMTP credentials under Settings → Credentials
   - **Power BI** (optional): OAuth2 credential for Power BI REST API

---

## Workflow 1 — Weekly Data Refresh

**Trigger:** Every Monday at 8:00 AM
**Purpose:** Download fresh CMS data, re-run ETL pipeline, refresh Power BI

```
[Cron Trigger]
    ↓
[HTTP Request: Download CMS Data]
    ↓
[Execute Command: Run ETL Pipeline]
    ↓
[SQLite: Verify Row Counts]
    ↓
[IF: Rows OK?]
    ├─ YES → [HTTP Request: Trigger Power BI Refresh]
    │            ↓
    │         [Send Email: Success Summary]
    └─ NO  → [Send Email: Pipeline Failure Alert]
```

### Node Configuration:

**Node 1 — Cron Trigger**
```json
{
  "type": "n8n-nodes-base.cron",
  "parameters": {
    "triggerTimes": {
      "item": [
        {
          "mode": "everyWeek",
          "hour": 8,
          "minute": 0,
          "weekday": "1"
        }
      ]
    }
  }
}
```

**Node 2 — HTTP Request: Download CMS Data**
```json
{
  "type": "n8n-nodes-base.httpRequest",
  "parameters": {
    "method": "GET",
    "url": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service/2022/data",
    "responseFormat": "file",
    "options": {
      "timeout": 300000
    }
  }
}
```
*Note: If CMS URL fails, this node is skipped and the pipeline uses existing data.*

**Node 3 — Execute Command: Run ETL Pipeline**
```json
{
  "type": "n8n-nodes-base.executeCommand",
  "parameters": {
    "command": "cd /path/to/physician-analytics && python src/data_pipeline.py && python src/sql_queries.py && python src/ml_clustering.py && python src/llm_insights.py"
  }
}
```
*Replace `/path/to/physician-analytics` with your actual path.*

**Node 4 — SQLite: Verify Row Counts**
```json
{
  "type": "n8n-nodes-base.sqlite",
  "parameters": {
    "operation": "executeQuery",
    "query": "SELECT COUNT(*) AS provider_count FROM providers; SELECT COUNT(*) AS payment_count FROM payments;",
    "databaseFile": "/path/to/physician-analytics/data/processed/physicians.db"
  }
}
```

**Node 5 — IF: Rows Sufficient**
```json
{
  "type": "n8n-nodes-base.if",
  "parameters": {
    "conditions": {
      "number": [
        {
          "value1": "={{$json[\"provider_count\"]}}",
          "operation": "larger",
          "value2": 1000
        }
      ]
    }
  }
}
```

**Node 6 — HTTP Request: Power BI Refresh** *(requires Power BI Premium)*
```json
{
  "type": "n8n-nodes-base.httpRequest",
  "parameters": {
    "method": "POST",
    "url": "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/refreshes",
    "authentication": "oAuth2",
    "oAuth2": "powerBiOAuth2",
    "sendHeaders": true,
    "headers": {
      "Content-Type": "application/json"
    }
  }
}
```

**Node 7 — Send Email: Weekly Summary**
```json
{
  "type": "n8n-nodes-base.emailSend",
  "parameters": {
    "fromEmail": "analytics@yourorg.com",
    "toEmail": "executives@yourorg.com",
    "subject": "Weekly Medicare Analytics Refresh — {{$now.format('YYYY-MM-DD')}}",
    "emailType": "text",
    "message": "The Medicare Physician Analytics pipeline completed successfully.\n\nProvider count: {{$node['SQLite'].json['provider_count']}}\nPayment records: {{$node['SQLite'].json['payment_count']}}\nRefresh time: {{$now}}\n\nView dashboard: [Power BI Link]"
  }
}
```

### Workflow JSON Export:
```json
{
  "name": "Weekly Medicare Data Refresh",
  "nodes": [
    {"name": "Cron Trigger", "type": "n8n-nodes-base.cron"},
    {"name": "Download CMS Data", "type": "n8n-nodes-base.httpRequest"},
    {"name": "Run ETL Pipeline", "type": "n8n-nodes-base.executeCommand"},
    {"name": "Verify Row Counts", "type": "n8n-nodes-base.sqlite"},
    {"name": "Rows OK?", "type": "n8n-nodes-base.if"},
    {"name": "Refresh Power BI", "type": "n8n-nodes-base.httpRequest"},
    {"name": "Send Success Email", "type": "n8n-nodes-base.emailSend"},
    {"name": "Send Failure Alert", "type": "n8n-nodes-base.emailSend"}
  ],
  "connections": {
    "Cron Trigger": {"main": [["Download CMS Data"]]},
    "Download CMS Data": {"main": [["Run ETL Pipeline"]]},
    "Run ETL Pipeline": {"main": [["Verify Row Counts"]]},
    "Verify Row Counts": {"main": [["Rows OK?"]]},
    "Rows OK?": {
      "main": [
        ["Refresh Power BI"],
        ["Send Failure Alert"]
      ]
    },
    "Refresh Power BI": {"main": [["Send Success Email"]]}
  }
}
```

---

## Workflow 2 — Daily Outlier Alert

**Trigger:** Every day at 9:00 AM
**Purpose:** Check for new or worsening outlier patterns; email alert if threshold exceeded

```
[Cron Trigger: Daily 9am]
    ↓
[SQLite: Count Outliers]
    ↓
[IF: outlier_count > threshold (200)?]
    ├─ YES → [SQLite: Get Top 10 Outliers]
    │            ↓
    │         [Format Message]
    │            ↓
    │         [Send Email: Outlier Alert]
    └─ NO  → [No Action]
```

### Node Configuration:

**Node 1 — Cron Trigger**
```json
{
  "type": "n8n-nodes-base.cron",
  "parameters": {
    "triggerTimes": {
      "item": [
        {
          "mode": "everyDay",
          "hour": 9,
          "minute": 0
        }
      ]
    }
  }
}
```

**Node 2 — SQLite: Count Outliers**
```json
{
  "type": "n8n-nodes-base.sqlite",
  "parameters": {
    "operation": "executeQuery",
    "query": "SELECT COUNT(*) AS outlier_count, ROUND(AVG(ABS(cost_z_score)), 2) AS avg_severity FROM payments WHERE is_outlier = 1",
    "databaseFile": "/path/to/physician-analytics/data/processed/physicians.db"
  }
}
```

**Node 3 — IF: Threshold Check**
```json
{
  "type": "n8n-nodes-base.if",
  "parameters": {
    "conditions": {
      "number": [
        {
          "value1": "={{$json[\"outlier_count\"]}}",
          "operation": "larger",
          "value2": 200
        }
      ]
    }
  }
}
```

**Node 4 — SQLite: Get Top 10 Outliers**
```json
{
  "type": "n8n-nodes-base.sqlite",
  "parameters": {
    "operation": "executeQuery",
    "query": "SELECT p.provider_name, p.specialty, p.state, ROUND(pay.avg_medicare_payment,2) AS cost, ROUND(pay.cost_z_score,2) AS z_score FROM providers p JOIN payments pay ON p.npi = pay.npi WHERE pay.is_outlier = 1 ORDER BY ABS(pay.cost_z_score) DESC LIMIT 10",
    "databaseFile": "/path/to/physician-analytics/data/processed/physicians.db"
  }
}
```

**Node 5 — Send Email: Outlier Alert**
```json
{
  "type": "n8n-nodes-base.emailSend",
  "parameters": {
    "fromEmail": "alerts@yourorg.com",
    "toEmail": "compliance@yourorg.com",
    "subject": "⚠️ Medicare Outlier Alert — {{$now.format('YYYY-MM-DD')}}",
    "emailType": "html",
    "message": "<h2>Outlier Physicians Detected</h2><p>Total outliers: <strong>{{$node['Count Outliers'].json['outlier_count']}}</strong></p><p>Avg severity: <strong>{{$node['Count Outliers'].json['avg_severity']}} std deviations</strong></p><h3>Top 10 Outliers:</h3><table border='1'>{{$node['Get Outliers'].json}}</table>"
  }
}
```

### Workflow JSON Export:
```json
{
  "name": "Daily Outlier Alert",
  "nodes": [
    {"name": "Daily Cron", "type": "n8n-nodes-base.cron"},
    {"name": "Count Outliers", "type": "n8n-nodes-base.sqlite"},
    {"name": "Above Threshold?", "type": "n8n-nodes-base.if"},
    {"name": "Get Top 10", "type": "n8n-nodes-base.sqlite"},
    {"name": "Send Alert Email", "type": "n8n-nodes-base.emailSend"}
  ],
  "connections": {
    "Daily Cron": {"main": [["Count Outliers"]]},
    "Count Outliers": {"main": [["Above Threshold?"]]},
    "Above Threshold?": {"main": [["Get Top 10"], []]},
    "Get Top 10": {"main": [["Send Alert Email"]]}
  }
}
```

---

## Workflow 3 — Provider Lookup Webhook

**Trigger:** HTTP POST to `/webhook/lookup`
**Purpose:** Accept NPI lookup requests and return full provider profile JSON

```
[Webhook: POST /lookup]
    ↓
[SQLite: Query Provider by NPI]
    ↓
[HTTP Request: FastAPI /provider/{npi}]
    ↓
[Respond to Webhook: Return JSON]
```

### Node Configuration:

**Node 1 — Webhook**
```json
{
  "type": "n8n-nodes-base.webhook",
  "parameters": {
    "httpMethod": "POST",
    "path": "lookup",
    "responseMode": "lastNode",
    "options": {}
  }
}
```
*Webhook URL will be: `https://your-n8n-instance.app.n8n.cloud/webhook/lookup`*

**Node 2 — SQLite: Query Provider**
```json
{
  "type": "n8n-nodes-base.sqlite",
  "parameters": {
    "operation": "executeQuery",
    "query": "SELECT p.*, q.cluster_name, q.performance_tier FROM providers p LEFT JOIN quality_metrics q ON p.npi = q.npi WHERE p.npi = '{{$json[\"body\"][\"npi\"]}}'",
    "databaseFile": "/path/to/physician-analytics/data/processed/physicians.db"
  }
}
```

**Node 3 — HTTP Request: FastAPI Summary**
```json
{
  "type": "n8n-nodes-base.httpRequest",
  "parameters": {
    "method": "GET",
    "url": "http://localhost:8000/provider/{{$node['Webhook'].json['body']['npi']}}",
    "options": {
      "timeout": 10000
    }
  }
}
```

**Node 4 — Respond to Webhook**
```json
{
  "type": "n8n-nodes-base.respondToWebhook",
  "parameters": {
    "respondWith": "json",
    "responseBody": "={{JSON.stringify($node['FastAPI Summary'].json)}}"
  }
}
```

### Usage Example:
```bash
curl -X POST https://your-n8n-instance.app.n8n.cloud/webhook/lookup \
  -H "Content-Type: application/json" \
  -d '{"npi": "1234567890"}'
```

### Workflow JSON Export:
```json
{
  "name": "Provider Lookup Webhook",
  "nodes": [
    {"name": "Webhook", "type": "n8n-nodes-base.webhook"},
    {"name": "Query DB", "type": "n8n-nodes-base.sqlite"},
    {"name": "Get API Summary", "type": "n8n-nodes-base.httpRequest"},
    {"name": "Return Response", "type": "n8n-nodes-base.respondToWebhook"}
  ],
  "connections": {
    "Webhook": {"main": [["Query DB"]]},
    "Query DB": {"main": [["Get API Summary"]]},
    "Get API Summary": {"main": [["Return Response"]]}
  }
}
```

---

## n8n.cloud Setup Guide

### 1. Create Account
- Go to https://app.n8n.cloud
- Sign up for free tier (up to 5 active workflows, 2,500 executions/month)

### 2. Import Workflows
1. Click **"+"** to create new workflow
2. Click the **"..."** menu → **Import from JSON**
3. Paste the JSON exports from above
4. Update all file paths and credentials

### 3. Configure Email Credentials
1. Go to **Settings → Credentials**
2. Click **"Add Credential" → Email (SMTP)**
3. Enter your SMTP settings (Gmail, Outlook, etc.)
4. Test the connection

### 4. Configure SQLite
- For n8n.cloud: The pipeline must run on the same server
- Recommended: Use a VPS (DigitalOcean, AWS EC2) to host both the pipeline and n8n
- Or use n8n's "Execute Command" node to SSH into the machine running the pipeline

### 5. Activate Workflows
1. Toggle each workflow to **"Active"** status
2. Test manually with "Execute Workflow" button
3. Monitor in **"Executions"** tab

### 6. Production Notes
- Store the SQLite DB path in n8n environment variables
- Use n8n's built-in error handling to catch failed executions
- Set up execution data pruning to avoid storage limits on free tier

---

## Screenshots Guide

### What to Screenshot for Documentation:
1. **Workflow 1**: Full canvas view showing all 7 nodes connected
2. **Workflow 2**: The IF node configuration with threshold setting
3. **Workflow 3**: Webhook URL display (blur the actual domain for security)
4. **Execution logs**: A successful execution showing green checkmarks on all nodes

*Screenshots would go in `outputs/n8n_screenshots/` directory*
