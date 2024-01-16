## Activate configuration

gcloud config configurations create cws
gcloud init

gcloud config configurations list
gcloud config configurations activate cws

## Deploy function

gcloud functions deploy run_attribution_model \
--runtime python310 \
--region europe-west3 \
--trigger-topic daily-topic \
--entry-point main \
--min-instances 1 \
--memory 512MB \
--timeout 540s

## Create pub / sub

gcloud pubsub topics create daily-topic

## Create schedule

gcloud scheduler jobs create pubsub daily-pubsub-job \
--schedule "0 9 \* \* \*" \
--topic projects/$(gcloud config get-value project)/topics/daily-topic \
--message-body "hello" \
--description "Daily Pub/Sub job at 9 AM"

## Documentation

If either of the notebooks is run outside of this folder, follow those steps before the first execution:

1. Create a local copy of Jeremy Nelson's library
   #!git clone https://github.com/jerednel/markov-chain-attribution.git

2. In order to get normalized output of the credit score, add the following variable to the first_order() function in **init**.py:
   credit_percentage=markov_conversions.copy()
   credit_percentage.update((x, y/total_conversions) for x, y in credit_percentage.items())

3. IMPORTANT - ELSE THE CODE WON'T WORK
   after the folder markov-chain-attribution is cloned into the project folder, rename that folder so that it doesn't contain hyphens.
   somehow, jupyter hub doesn't import local packages if their path contains hyphens (-).
