import pandas as pd
import numpy as np
import pandas_gbq
import os 
from google.cloud import bigquery
import functions_framework

from markov_chain_attribution import markov_model_attribution as mma



@functions_framework.cloud_event
def main(cloud_event):
    bigquery_client = bigquery.Client()
    #VERSION 2 OF THE EXTRACTION QUERY. medium/source grouping updated following comments from CWS from 15.06.23
    #uncomment below to execute

    #Defining query that reads out, cleanses, and transforms data from the BigQuery account of CWS
    #query  below contains data only from Google Analytics 4

    query_alldates="""
    #users might have multiple conversions within their lifecycle
    #ec CTE extracts the earliest conversion date for each user_pseudo_id
    with ec as
    (select user_pseudo_id, min(event_timestamp) earliest_conv_timestamp from `cws-data.dataform_staging.form_submit`
    where form_name='form_lead'
    group by user_pseudo_id),
    #f CTE extracts all relevant data from the form_submit table. Submission of any form other than "form_lead" is discarded
    f as
    (select user_pseudo_id, session_id, date, solution_area, form_name
    from `cws-data.dataform_staging.form_submit`
    where form_name='form_lead'),
    #some session_id's in the sessions table are duplicated, yet with different session_numbers. in order to attach the conversion event to the latest of those duplicate sessions, below we create the lsn CTE which contains the latest session_number per each session_id
    lsn as
    (select session_id, max(session_number) as latest_session_number from `cws-data.dataform_staging.sessions`
    group by session_id),
    #sc CTE extracts all relevant fields from the sessions table, combining values from the medium and source fields under a standardized channel field
    sc as
    (select s.user_pseudo_id,
    s.session_id, s.session_start, #s.session_end,
    s.session_number,
    #s.country, s.device_category, s.language,
    #s.session_page_location, s.session_page_referrer,
    #s.last_channel_info.medium medium, s.last_channel_info.source source,
    case when ((s.last_channel_info.medium in ('c','cp','cpc','gcpc','Paid_Display', 'Display', null) and s.last_channel_info.source in ('google','g','googl','goog','go','goo','Google', 'Outbrain', 'URL', 'adwords')) or (s.last_channel_info.medium is null and s.last_channel_info.source in ('adwords'))) then 'Google Paid'
    when (s.last_channel_info.medium in ('GMB') and s.last_channel_info.source in ('Google')) then 'Organic GMB'
    when (s.last_channel_info.medium in ('Facebook', 'social', 'Social', 'post', 'LinkedIn', 'Poster') and s.last_channel_info.source in ('Social', 'facebook', 'TikTok', 'linkedin', 'post', 'LinkedIn', 'linkedin.com')) then 'Organic Social'
    when (s.last_channel_info.medium in ('paidsocial', 'SocialPaid', 'cpc', 'facebook') and s.last_channel_info.source in ('facebook%2Cinstagram%2Cmessenger', 'facebook%2Cinstagram%2Cmessenge', 'facebook%2Cinstagram%', 'facebook%2Cinstagram%2Caudience_network%2Cmessenger', 'facebook%2Cinstagram%2Caudience_network', 'facebook,instagram', 'fb', 'facebook%2Cinstagram', 'f', 'instagram%2Cfacebook', 'Facebook', 'Facebook%26Instagram', 'Facebook', 'facebook,instagram,audience_network,messenger', 'dlvr.it')) then 'Facebook Paid'
    when (s.last_channel_info.medium in ('cpc') and s.last_channel_info.source in ('bing')) then 'Bing Paid'
    when (s.last_channel_info.medium in ('cpc') and s.last_channel_info.source in ('tiktok')) then 'TikTok Paid'
    when (s.last_channel_info.medium in ('Recruiting') and s.last_channel_info.source in ('GoogleDisplay')) then 'Recruiting Google'
    when (s.last_channel_info.medium in ('Recruiting') and s.last_channel_info.source in ('Youtube')) then 'Recruiting YouTube'
    when (s.last_channel_info.medium in ('Meta') and s.last_channel_info.source in ('Recruiting%20Campaign%202023','Recruiting+Campaign+2023')) then 'Recruiting Meta'
    when (s.last_channel_info.medium in ('none') and s.last_channel_info.source in ('direct')) then 'Direct'
    when (s.last_channel_info.medium in ('LinkedIn') and s.last_channel_info.source in ('Recruiting%20Campaign%202023')) then 'Recruiting LinkedIn'
    when (s.last_channel_info.medium in ('cpc') and s.last_channel_info.source in ('linkedin')) then 'LinkedIn Paid'
    when (s.last_channel_info.medium in ('organic','Organic') and s.last_channel_info.source in ('bing', 'google', 'facebook,instagram', 'tiktok', 'linkedin', 'Vacatures.nl')) then 'Organic Search'
    when (s.last_channel_info.medium in ('email','mail','image','intext','autosvar','button','autosvar_kundtjanst','Pardot') and s.last_channel_info.source in ('mailing','newsletter_53','newsletter','newsletter_54','employee','newsletter_55','Mail','mail','email', 'salesforce','EmailCustomers','sendinblue','CleverReach','newsletter_29','newsletter_31','newsletter_24','M%26I+RI+Contacts','hs_email','getresponse','emarsys','newsletter_30','newsletter_44','mai')) then 'Email'
    when (s.last_channel_info.medium in ('Banner','bildl%C3%A4nk','catalogue','flyer','ad','banner_2') and s.last_channel_info.source in ('CSU','Roularta','padelexpo+webb','print','conventus')) then 'Offline'
    when ((s.last_channel_info.medium in ('referral', 'Referral')) or (s.last_channel_info.medium in ('Ang-Rg','flyer','internet','native','null','TT','article','pressrelease', 'anhaenger') and s.last_channel_info.source in ('fs-de','web','fcrmedia','aktuell_hallbarhet','DN','uatalents','pressrelease','produktsida','najmama.sk','TT', 'fs-de-DE'))) then 'Referral'
    when (s.last_channel_info.medium in ('JobPosting') and s.last_channel_info.source in ('professional.ch')) then 'Recruiting Referral'
    when (s.last_channel_info.medium in ('discover') and s.last_channel_info.source in ('newsshowcase')) then 'Discover'
    when ((s.last_channel_info.medium is null or s.last_channel_info.medium in ('c', 'cpc', 'logo')) and (s.last_channel_info.source is null or s.last_channel_info.source in ('lp', 'facebook', 'www.rosinenpicker.de', 'www.bayern.jobs', 'facebook,instagram,messenger', 'pocket_mylist', 'google', 'Google', 'baulinks', 'www.maz-job.de', 'uw-stad-werkt', 'shntch', 'gnntch', 'www.meine.jobs', 'bing', 'tb', 'dustom', 'facebook,instagram,audience_network,messenger', 'duntch'))) then 'Needs Review'
    else 'NEW CHANNEL'
    end as channel
    from `cws-data.dataform_staging.sessions` s
    where (s.last_channel_info.medium, s.last_channel_info.source) not in (('test', 'test'), ('testmed', 'testsource'), ('organic', 'google_jobs_apply'), ('organic', 'automail-job-push'), ('JobPosting', 'professional.ch'), ('Recruiting', 'GoogleDisplay'), ('organic', 'google_jobs_apply'), ('Recruiting', 'Youtube'), ('Meta', 'Recruiting%20Campaign%202023'), ('Meta', 'Recruiting+Campaign+2023'), ('LinkedIn', 'Recruiting%20Campaign%202023'))), 
    #excluding test source/media, as well as those that are not relevant to form_lead conversion event
    #fsallnj CTE joins the table with conversions data (f) and the table with sessions data with standardized channel names (sc).
    #in case sessions with conversions have duplicate rows, conversion record is added to the session with the latest session_number (lsn).
    #User journeys are not constructed in this CTE
    fsallnj as
    (select sc.user_pseudo_id,
    sc.session_id, sc.session_start, #sc.session_end,
    sc.session_number,
    #sc.country, sc.device_category, sc.language,
    #sc.session_page_location, sc.session_page_referrer,
    #sc.medium, sc.source,
    sc.channel, case when (sc.session_id=f.session_id) then 'conv' else 'null' end as conversion
    from sc
    left join lsn
    on sc.session_id=lsn.session_id
    left join f
    on sc.session_id=f.session_id and sc.session_number=lsn.latest_session_number),
    #in fsnj all rows with conversions that happened after the first conversion are discarded (left join of the ec CTE)
    fsnj as
    (select fsallnj.user_pseudo_id,
    fsallnj.session_id, fsallnj.session_start, #fsallnj.session_end,
    fsallnj.session_number,
    #fsallnj.country, fsallnj.device_category, fsallnj.language,
    #fsallnj.session_page_location, fsallnj.session_page_referrer,
    #fsallnj.medium, fsallnj.source,
    fsallnj.channel, fsallnj.conversion from fsallnj
    left join ec on fsallnj.user_pseudo_id=ec.user_pseudo_id
    where fsallnj.session_start<coalesce (ec.earliest_conv_timestamp, current_timestamp())
    group by fsallnj.user_pseudo_id,
    fsallnj.session_id, fsallnj.session_start, #fsallnj.session_end,
    fsallnj.session_number,
    #fsallnj.country, fsallnj.device_category, fsallnj.language,
    #fsallnj.session_page_location, fsallnj.session_page_referrer,
    #fsallnj.medium, fsallnj.source,
    fsallnj.channel, fsallnj.conversion),
    #fsjnc CTE combines all channels that user interacted with into one long string (journey_nc). At this step, conversion is not included in that journey string.
    fsjnc as
    (select user_pseudo_id, string_agg(channel, " > " order by session_number asc) as journey_nc from fsnj
    group by user_pseudo_id),
    #fscon CTE returns a list of all user_pseudo_id's with an indication of whether that user converted or not
    fscon as
    (SELECT user_pseudo_id, IF(COUNTIF(conversion = 'conv') > 0, 'conv', 'null') converted FROM fsnj
    GROUP BY user_pseudo_id),
    #below is the final table that contains pseudo_user_id's and their respecive journeys.
    #In case customer's journey resulted in a form_lead submission, the journey ends with 'conv'. Otherwise the journey ends with 'null'
    fspath as (select fsjnc.user_pseudo_id, concat('start', ' > ', fsjnc.journey_nc, ' > ', fscon.converted) path from fsjnc
    left join fscon on fsjnc.user_pseudo_id=fscon.user_pseudo_id)
    select path from fspath
    """



    #Run the query above and write result to a pandas dataframe

    query_results = bigquery_client.query(query_alldates)
    df_alldates =query_results.to_dataframe()

    print("bigquery results: ",len(df_alldates))
    return
    #View top few rows of result
    #df_alldates.head()



    #feeding data into the attribution model
    model_alldates = mma.run_model(paths=df_alldates)



    #sorting channels alphabetically
    credit_scores_alldates=dict(sorted(model_alldates['credit_percentages'].items(), key=lambda item: item[1], reverse=True))


    #rounding the credit scores to the second decimal
    rounded_credit_scores_alldates = credit_scores_alldates.copy()
    for key in rounded_credit_scores_alldates.keys():
        rounded_credit_scores_alldates[key] = round(100*rounded_credit_scores_alldates[key], 2)
    #rounded_credit_scores_alldates



    # preparing the model output for writing into BigQuery

    #convering the model output from dictionary into dataframe
    df_bigquery_alldates=pd.DataFrame(list(rounded_credit_scores_alldates.items()), columns=['channel', 'credit_score'])

    #finding all columns of object datatype and converting them into strings - since BigQuery doesn't support object datatype
    object_columns_alldates = df_bigquery_alldates.select_dtypes(include=['object']).columns
    if not object_columns_alldates.empty:
        df_bigquery_alldates[object_columns_alldates] = df_bigquery_alldates[object_columns_alldates].astype(pd.StringDtype())
        
    df_bigquery_alldates['date_range']='alldates'



    #writing the dataframe into a BigQuery table (using pandas_gbq, see https://pandas-gbq.readthedocs.io/en/latest/writing.html)
    pandas_gbq.to_gbq(
        df_bigquery_alldates, 'dataform_staging.attributionmodeloutput', project_id='cws-data', if_exists='replace',
    )

    print("Results inserted in BigQuery")

    #testing - saving output as a csv
    #df_bigquery_alldates.to_csv('am_test.csv', sep=';')
