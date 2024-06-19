import requests
import pandas as pd
from gcsa.event import Event
from gcsa.google_calendar import GoogleCalendar
import os
from googleapiclient.errors import HttpError
import boto3

# Let's use Amazon S3
s3 = boto3.client("s3")

MTA_URL = (
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"
)
CALENDAR_ID = os.environ.get("CALENDAR_ID")

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")


EXISTING_DF_FILENAME = "mta_l_alerts.parquet"
CREDENTIALS_JSON_FILENAME = "google-oath-credentials-secret.json"
TOKEN_FILENAME = "token.pickle"

LOCAL_DIR = "/tmp"


PUSHOVER_APP_TOKEN = os.environ.get("PUSHOVER_APP_TOKEN")
PUSHOVER_USER_KEY = os.environ.get("PUSHOVER_USER_KEY")


def send_pushover_message(message, title=None, url=None):
    payload = {
        "token": PUSHOVER_APP_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "message": message,
    }
    if title:
        payload["title"] = title
    if url:
        payload["url"] = url
    requests.post("https://api.pushover.net/1/messages.json", data=payload)


def lambda_handler(event, context):
    try:
        return lambda_handler_actual(event, context)
    except Exception as e:
        send_pushover_message(
            f"Error in lambda function: {repr(e)}",
            title="Error in L Service Alert Lambda Function",
        )
        raise e


def lambda_handler_actual(event, context):
    # print(PUSHOVER_APP_TOKEN, PUSHOVER_USER_KEY, CALENDAR_ID, S3_BUCKET_NAME)
    # raise Exception("Test Error")
    # test = 40/0
    if "trigger" in event:
        if event["trigger"] == "schedule":
            print("Scheduled Trigger")
        else:
            print(f"trigger : {event['trigger']}")
    os.makedirs(LOCAL_DIR, exist_ok=True)

    EXISTING_DF_PATH = os.path.join(LOCAL_DIR, EXISTING_DF_FILENAME)
    CREDENTIALS_JSON_PATH = os.path.join(LOCAL_DIR, CREDENTIALS_JSON_FILENAME)
    TOKEN_PATH = os.path.join(LOCAL_DIR, TOKEN_FILENAME)

    print("Downloading files from S3")
    s3.download_file(S3_BUCKET_NAME, CREDENTIALS_JSON_FILENAME, CREDENTIALS_JSON_PATH)
    s3.download_file(S3_BUCKET_NAME, TOKEN_FILENAME, TOKEN_PATH)
    s3.download_file(S3_BUCKET_NAME, EXISTING_DF_FILENAME, EXISTING_DF_PATH)

    print("connecting to google calendar")
    gc = GoogleCalendar(credentials_path=CREDENTIALS_JSON_PATH, token_path=TOKEN_PATH)

    # Load existing DF
    print("Loading existing data")
    existingDF = pd.read_parquet(EXISTING_DF_PATH)

    print("Pulling new data from MTA")
    resp = requests.get(MTA_URL)

    entries = resp.json()["entity"]

    df = pd.DataFrame(entries)
    # df

    df[["type", "mta_id"]] = df["id"].str.split(":", expand=True)[[1, 2]]
    df = df.set_index("mta_id")
    # df

    df = df[df["type"] == "planned_work"]
    # df

    extracted = df.apply(
        lambda row: row["alert"]["transit_realtime.mercury_alert"],
        axis=1,
        result_type="expand",
    )
    # NOTE: an error could occur here if there were no alerts that had the columns we look for
    df[extracted.columns] = extracted
    # df

    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s")
    # df

    df["route_id"] = df.apply(
        lambda row: row["alert"]["informed_entity"][0]["route_id"], axis=1
    )
    # df

    # Filter to just the L train
    df = df[df["route_id"] == "L"].copy()
    # df

    # And just planned suspension
    df = df[df["alert_type"] == "Planned - Part Suspended"].copy()
    # df

    df["active_period"] = df.apply(lambda row: row["alert"]["active_period"], axis=1)
    # df

    df["header_en"] = df.apply(
        lambda row: list(
            filter(
                lambda x: x["language"] == "en",
                row["alert"]["header_text"]["translation"],
            )
        )[0]["text"],
        axis=1,
    )
    # df
    df["header_en-html"] = df.apply(
        lambda row: list(
            filter(
                lambda x: x["language"] == "en-html",
                row["alert"]["header_text"]["translation"],
            )
        )[0]["text"],
        axis=1,
    )

    df["description_en"] = df.apply(
        lambda row: list(
            filter(
                lambda x: x["language"] == "en",
                row["alert"]["description_text"]["translation"],
            )
        )[0]["text"],
        axis=1,
    )
    df["description_en-html"] = df.apply(
        lambda row: list(
            filter(
                lambda x: x["language"] == "en-html",
                row["alert"]["description_text"]["translation"],
            )
        )[0]["text"],
        axis=1,
    )

    def convertActiveTimes(row):
        newItems = []
        for item in row["active_period"]:
            newItem = {
                "start": pd.to_datetime(item["start"], unit="s", utc=True).tz_convert(
                    "America/New_York"
                ),
                "end": pd.to_datetime(item["end"], unit="s", utc=True).tz_convert(
                    "America/New_York"
                ),
            }
            newItems.append(newItem)
        return newItems

    df["active_period_dt"] = df.apply(convertActiveTimes, axis=1)
    df["event_ids"] = None  # Need this so no errors

    print(f"Successfully Parsed {len(df)} events from  MTA Data")
    newEvents = 0
    updatedEvents = 0
    deleted_events = 0

    def create_cal_events(row):
        event_ids = []
        print(row["active_period_dt"])
        for period in row["active_period_dt"]:
            event = Event(
                row["header_en"],
                start=period["start"],
                end=period["end"],
                description=row["description_en-html"],
            )
            eventRet = gc.add_event(event, calendar_id=CALENDAR_ID)
            event_ids.append(eventRet.id)
        # event_ids
        return event_ids

    def delete_events(event_ids):
        for event_id in event_ids:
            print(f"Deleting event {event_id}")
            try:
                gc.delete_event(event_id, calendar_id=CALENDAR_ID)
            except HttpError as e:
                print(f"Error deleting event {event_id}: {e}")

    print("Going through events")
    for idx, row in df.iterrows():
        if idx in existingDF.index:
            existingRow = existingDF.loc[idx]

            if existingRow["updated_at"] < row["updated_at"]:
                print(f"Updating {idx}")
                delete_events(existingRow["event_ids"])
                df.at[idx, "event_ids"] = create_cal_events(row)
                existingDF = existingDF.drop(idx)
                existingDF = pd.concat([existingDF, df.loc[[idx]]])
                updatedEvents += 1
            else:
                print(f"No update needed for {idx}")
        else:
            print(f"Creating {idx}")
            df.at[idx, "event_ids"] = create_cal_events(row)
            existingDF = pd.concat([existingDF, df.loc[[idx]]])
            newEvents += 1

    def getLatestEnd(dateArr):
        latestEnd = dateArr[0]["end"]
        for date in dateArr:
            if date["end"] > latestEnd:
                latestEnd = date["end"]
        return latestEnd

    # These are events not in the feed anymore
    print("looking at extra entries (entries not in feed anymore)")
    extraEntriesDF = existingDF[~existingDF.index.isin(df.index)].copy()
    extraEntriesDF["latestEnd"] = extraEntriesDF["active_period_dt"].apply(getLatestEnd)


    for idx, row in extraEntriesDF.iterrows():
        # If already over, keep otherwise delete
        if row["latestEnd"] < pd.Timestamp.now(tz="America/New_York"):
            print(f"{idx} missing from feed but already over, keeping it")
        else:
            print(f"{idx} missing from feed, deleting it")
            delete_events(row["event_ids"])
            existingDF = existingDF.drop(idx)
            deleted_events += 1

    print(
        f"Created {newEvents} new events, updated {updatedEvents} events, and deleted {deleted_events} events"
    )

    existingDF.to_parquet(EXISTING_DF_PATH)
    print("Successfully updated calendar")
    print("Uploading new data to S3")

    s3.upload_file(EXISTING_DF_PATH, S3_BUCKET_NAME, EXISTING_DF_FILENAME)
    print("Successfully uploaded new data to S3")
    return {
        "statusCode": 200,
        "body": "Successfully updated calendar",
        "created_count": newEvents,
        "updated_count": updatedEvents,
        "deleted_count": deleted_events,
    }
