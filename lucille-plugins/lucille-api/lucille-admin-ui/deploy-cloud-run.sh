#!/bin/bash

# Build and deploy to Google Cloud Run
gcloud run deploy lucille-admin-ui \
  --source . \
  --platform managed \
  --region us-east1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --max-instances=3
