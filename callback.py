import datetime
import json
import os
import sys
import time

import pandas as pd
from bpm.bambuconfig import BambuConfig
from bpm.bambuprinter import BambuPrinter
from bpm.bambutools import parseFan, parseStage
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SPREADSHEET_ID = "1vk4Im7TahIPYzG3kIxSKjpDKbko_QkMyfFuhzYoSLjc"
BOOKING_SHEET = "Booking"
STARTING_SHEET = "Starting"
STATUS_SHEET = "Printer Status"
LIMITS_SHEET = "Filament Limits"

booking_data = None
starting_data = None
status_data = None
limits_data = None

booking_statuses = [
    "Waiting for Printer",
    "Currently Printing",
    "Did Not Start Print",
    "Print Done",
    "Not Certified",
]

printer_statuses = ["Booked", "Printing", "Available", "Offline"]

try:
    printer_data = json.load(open("printers.json"))
except FileNotFoundError:
    print("No printers.json file found.")
    exit(1)

printers = []

creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
elif not os.path.exists("credentials.json"):
    print("No credentials.json file found.")
    exit(1)
# If there are no (valid) credentials available, let the user log in (assuming credentials.json exists).
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=44649)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
        token.write(creds.to_json())

try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    g_sheets = service.spreadsheets()

except HttpError as e:
    print(e)
    exit(1)


def get_sheet_data():
    global booking_data, starting_data, status_data, limits_data
    try:
        booking = (
            g_sheets.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=BOOKING_SHEET)
            .execute()
        )

        values = booking.get("values", [])

        if not values:
            print("No booking data found.")
            exit(1)

        values = [r + [""] * (len(values[0]) - len(r)) for r in values]

        booking_data = pd.DataFrame(
            values[1:] if len(values) > 1 else None,
            columns=values[0],
        )

        starting = (
            g_sheets.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=STARTING_SHEET)
            .execute()
        )

        values = starting.get("values", [])

        if not values:
            print("No starting data found.")
            exit(1)

        values = [r + [""] * (len(values[0]) - len(r)) for r in values]

        starting_data = pd.DataFrame(
            values[1:] if len(values) > 1 else None,
            columns=values[0],
        )

        status = (
            g_sheets.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=STATUS_SHEET)
            .execute()
        )

        values = status.get("values", [])

        if not values:
            print("No status data found.")
            exit(1)

        values = [r + [""] * (len(values[0]) - len(r)) for r in values]

        status_data = pd.DataFrame(
            values[1:] if len(values) > 1 else None,
            columns=values[0],
        )

        limits = (
            g_sheets.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=LIMITS_SHEET)
            .execute()
        )

        values = limits.get("values", [])

        if not values:
            print("No limits data found.")
            exit(1)

        values = [r + [""] * (len(values[0]) - len(r)) for r in values]

        limits_data = pd.DataFrame(
            values[1:] if len(values) > 1 else None,
            columns=values[0],
        )

    except HttpError as e:
        print(e)
        exit(1)


def write_status_sheet():
    try:
        vals = status_data.values.tolist()
        vals.insert(0, status_data.columns.tolist())
        # length = len(vals)
        # blank_filled = 0

        # if student_sheet_read_len > length:
        #     blank_filled = student_sheet_read_len - length
        #     vals = vals + [[""] * len(student_data.columns)] * (blank_filled)
        # else:
        #     student_sheet_read_len = length

        # for i in range(0, student_sheet_read_len, SEND_BLOCK):
        _ = (
            g_sheets.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=STATUS_SHEET,
                valueInputOption="USER_ENTERED",
                body={"values": vals},
            )
            .execute()
        )
        # student_sheet_read_len -= blank_filled
        return True
    except HttpError as e:
        print(e)
        return False


if __name__ == "__main__":

    get_sheet_data()

    print(booking_data)
    print()
    print(starting_data)
    print()
    print(status_data)
    print()
    print(limits_data)
    print()

    for name in printer_data:
        p = printer_data[name]
        if "hostname" not in p or "access_code" not in p or "serial_number" not in p:
            print(
                f"Error: printer config for {name} missing hostname, access_code, or serial_number"
            )
            sys.exit(1)

        config = BambuConfig(
            hostname=p["hostname"],
            access_code=p["access_code"],
            serial_number=p["serial_number"],
        )
        printer = BambuPrinter(config=config)
        printers.append((name, printer))
        printer.start_session()

    printers.sort(
        key=lambda x: status_data.loc[status_data["Printer Name"] == x[0]].index[0]
    )

    # print(printers)

    while True:
        for i, (printer_name, printer) in enumerate(printers):
            print(f"Printer {i}: {printer_name}")

            if printer._lastMessageTime:
                print(
                    f"last checkin: {round(time.time() - printer._lastMessageTime)}s ago"
                )

            print(
                f"tool=[{round(printer.tool_temp, 1)}/{round(printer.tool_temp_target, 1)}] "
                + f"bed=[{round(printer.bed_temp, 1)}/{round(printer.bed_temp_target, 1)}] "
                + f"fan=[{parseFan(printer.fan_speed)}] print=[{printer.gcode_state}] speed=[{printer.speed_level}] "
                + f"light=[{'on' if printer.light_state else 'off'}]"
            )
            print(
                f"stg_cur=[{parseStage(printer.current_stage)}] file=[{printer.gcode_file}] "
                + f"layer=[{printer.current_layer}/{printer.layer_count}] "
                + f"%=[{printer.percent_complete}] eta=[{printer.time_remaining} min] "
                + f"spool=[{printer.active_spool} ({printer.spool_state})]"
            )
            print()

            if printer.gcode_state in ["RUNNING", "PAUSE"]:
                status_data.loc[i, "Status"] = printer_statuses[1]
                status_data.loc[
                    i, "End Time"
                ] = (datetime.datetime.now() + datetime.timedelta(
                    minutes=printer.time_remaining
                )).strftime("%Y-%m-%d %H:%M")

        print()
        write_status_sheet()

        time.sleep(1)
