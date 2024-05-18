import datetime
import json
import os
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

import sheet

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
    "Booked Printer",
    "Currently Printing",
    "Did Not Start Print",
    "Print Done",
    "Not Certified",
]

printer_statuses = ["Booked", "Printing", "Available", "Offline"]

booking_index = -1

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


def write_booking_sheet():
    try:
        vals = booking_data.values.tolist()
        vals.insert(0, booking_data.columns.tolist())
        _ = (
            g_sheets.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=BOOKING_SHEET,
                valueInputOption="USER_ENTERED",
                body={"values": vals},
            )
            .execute()
        )
        return True
    except HttpError as e:
        print(e)
        return False


def write_status_sheet():
    try:
        vals = status_data.values.tolist()
        vals.insert(0, status_data.columns.tolist())
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
        return True
    except HttpError as e:
        print(e)
        return False


def write_limits_sheet():
    try:
        vals = limits_data.values.tolist()
        vals.insert(0, limits_data.columns.tolist())
        _ = (
            g_sheets.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=LIMITS_SHEET,
                valueInputOption="USER_ENTERED",
                body={"values": vals},
            )
            .execute()
        )
        return True
    except HttpError as e:
        print(e)
        return False


def update_printer_status(
    printer_num: int | None,
    status_num: int | None,
    user: str | None,
    start_time: datetime.datetime | str | None,
    end_time: datetime.datetime | str | None,
):
    if status_num:
        status_data.loc[printer_num, "Status"] = printer_statuses[status_num]
    if user:
        status_data.loc[printer_num, "Current User"] = user
    if start_time:
        if type(start_time) == datetime.datetime:
            status_data.loc[printer_num, "Start Time"] = start_time.strftime(
                "%Y-%m-%d %H:%M"
            )
        else:
            status_data.loc[printer_num, "Start Time"] = start_time
    if end_time:
        if type(end_time) == datetime.datetime:
            status_data.loc[printer_num, "End Time"] = end_time.strftime(
                "%Y-%m-%d %H:%M"
            )
        else:
            status_data.loc[printer_num, "End Time"] = end_time


if __name__ == "__main__":
    try:
        sheet.get_sheet_data(False)
        get_sheet_data()

        # set up printers
        for name in printer_data:
            p = printer_data[name]
            if (
                "hostname" not in p
                or "access_code" not in p
                or "serial_number" not in p
            ):
                print(
                    f"Error: printer config for {name} missing hostname, access_code, or serial_number"
                )
                exit(1)

            config = BambuConfig(
                hostname=p["hostname"],
                access_code=p["access_code"],
                serial_number=p["serial_number"],
            )
            printer = BambuPrinter(config=config)
            printers.append((name, printer))
            printer.start_session()

        # sort printers by their order in the status sheet
        printers.sort(
            key=lambda x: status_data.loc[status_data["Printer Name"] == x[0]].index[0]
        )

        waiting_for_printer = []
        currently_booked_or_printing = []
        while True:
            get_sheet_data()

            # TODO: check for people who have started prints in the last 10 minutes

            complete_prints = []
            for i, (printer_name, printer) in enumerate(printers):
                # print(f"Printer {i}: {printer_name}")

                # if printer._lastMessageTime:
                #     print(
                #         f"last checkin: {round(time.time() - printer._lastMessageTime)}s ago"
                #     )
                # print(f"print=[{printer.gcode_state}]")

                if printer.gcode_state in ["RUNNING", "PAUSE"]:
                    # if printer is currently printing

                    user = status_data.loc[i, "Current User"]
                    # TODO: check for verifications/start sheet to determine who started print

                    update_printer_status(
                        i,
                        1,
                        user,
                        datetime.datetime.fromtimestamp(printer.start_time * 60),
                        datetime.datetime.now()
                        + datetime.timedelta(minutes=printer.time_remaining),
                    )
                elif status_data.loc[i, "Status"] == printer_statuses[1]:
                    # if printer just finished printing
                    complete_prints.append(status_data.loc[i, "Current User"])
                    update_printer_status(i, 2, "", "", "")
                elif status_data.loc[i, "Status"] not in printer_statuses:
                    # if printer is not printing but no valid status is recorded
                    update_printer_status(i, 2, "", "", "")

                # if printer is available and someone is waiting for it
                if (
                    status_data.loc[i, "Status"] == printer_statuses[2]
                    and waiting_for_printer
                ):
                    end_time = datetime.datetime.now() + datetime.timedelta(hours=4)
                    if end_time.hour >= 21:
                        # 3 hours + 12 hours for next day from 9pm to 12pm
                        end_time = end_time + datetime.timedelta(hours=3 + 12)
                    row, user = waiting_for_printer.pop(0)
                    update_printer_status(
                        i,
                        0,
                        user,
                        datetime.datetime.now(),
                        end_time,
                    )
                    currently_booked_or_printing.append(user)
                    booking_data.loc[row, "Status"] = booking_statuses[1]

            first_active_index = -1

            for i, row in booking_data.iterrows():
                if i <= booking_index:
                    continue

                cruzid = row["Email Address"].split("@")[0]
                if row["Status"] in ["", booking_statuses[0]]:
                    if sheet.is_staff(cruzid=cruzid) or sheet.get_access(
                        "3D Printing", cruzid=cruzid
                    ):
                        row["Status"] = booking_statuses[0]
                        if cruzid not in currently_booked_or_printing:
                            waiting_for_printer.append((i, cruzid))
                    else:
                        row["Status"] = booking_statuses[5]
                elif row["Status"] == booking_statuses[2] and cruzid in complete_prints:
                    row["Status"] = booking_statuses[4]
                    complete_prints.remove(cruzid)
                    currently_booked_or_printing.remove(cruzid)

                if first_active_index == -1 and row["Status"] in booking_statuses[0:3]:
                    first_active_index = i
                    booking_index = i - 1

            print(waiting_for_printer)

            print()
            write_booking_sheet()
            write_status_sheet()

            time.sleep(10)
    except KeyboardInterrupt:
        exit(0)
