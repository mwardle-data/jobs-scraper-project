from google.cloud import bigquery

client = bigquery.Client()
print("Hurray, the connection works!", client.project)
