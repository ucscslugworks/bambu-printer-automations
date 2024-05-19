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
    "Supervised Printing",
    "Did Not Start Print",
    "Print Done",
    "Not Certified",
]

USER_WAITING = 0
USER_BOOKED = 1
USER_PRINTING = 2
USER_SUPERVISED = 3
USER_NO_START = 4
USER_DONE = 5
USER_NOT_CERTIFIED = 6

printer_statuses = ["Booked", "Printing", "Available", "Offline", "Cancel Pending"]

PRINTER_BOOKED = 0
PRINTER_PRINTING = 1
PRINTER_AVAILABLE = 2
PRINTER_OFFLINE = 3
PRINTER_CANCEL_PENDING = 4

booking_index = 0

BOOKING_TIME = 4  # hours
MAX_TOOL_TEMP = 220  # degrees Celsius
TIME_TO_START = 10  # minutes

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


def write_starting_sheet():
    try:
        vals = starting_data.values.tolist()
        vals.insert(0, starting_data.columns.tolist())
        _ = (
            g_sheets.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=STARTING_SHEET,
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
    if status_num is not None:
        status_data.loc[printer_num, "Status"] = printer_statuses[status_num]
    if user is not None:
        status_data.loc[printer_num, "Current User"] = user
    if start_time is not None:
        if type(start_time) == datetime.datetime:
            status_data.loc[printer_num, "Start Time"] = start_time.strftime(
                "%Y-%m-%d %H:%M"
            )
        else:
            status_data.loc[printer_num, "Start Time"] = start_time
    if end_time is not None:
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

        if len(printers) != len(status_data):
            print(
                f"Error: number of printers in printers.json ({len(printers)}) does not match number of printers in status sheet ({len(status_data)})"
            )
            exit(1)

        # sort printers by their order in the status sheet
        printers.sort(
            key=lambda x: status_data.loc[status_data["Printer Name"] == x[0]].index[0]
        )

        waiting_for_printer = []  # users who are waiting for printer
        waiting_for_printer_rows = (
            dict()
        )  # row numbers (in booking_data) of users who are waiting for printer
        currently_booked_or_printing = []  # users who are currently booked or printing
        currently_booked_or_printing_rows = (
            dict()
        )  # row numbers (in booking_data) of users who are currently booked or printing

        print_without_booking = (
            []
        )  # printers that have started prints without a booking
        print_without_booking_data = (
            dict()
        )  # data for printers that have started prints without a booking - printer num, start time
        print_with_booking = []  # printers that have started prints with a booking
        print_with_booking_data = (
            dict()
        )  # data for printers that have started prints with a booking - printer num, user, row number in booking_data, start time

        while True:
            get_sheet_data()
            timestamp = datetime.datetime.now()

            complete_prints = []

            for i, (printer_name, printer) in enumerate(printers):
                # print(f"Printer {i}: {printer_name}")

                # if printer._lastMessageTime:
                #     print(
                #         f"last checkin: {round(time.time() - printer._lastMessageTime)}s ago"
                #     )
                # print(f"print=[{printer.gcode_state}]")

                # if status_data.loc[i, "Status"] == printer_statuses[PRINTER_PRINTING]:
                #     user = status_data.loc[i, "Current User"]
                #     print(
                #         timestamp,
                #         timestamp - datetime.timedelta(minutes=TIME_TO_START),
                #         datetime.datetime.strptime(
                #             status_data.loc[i, "Start Time"].strip() + ":00",
                #             "%Y-%m-%d %H:%M:%S",
                #         ),
                #         datetime.datetime.strptime(
                #             status_data.loc[i, "Start Time"].strip() + ":00",
                #             "%Y-%m-%d %H:%M:%S",
                #         )
                #         <= timestamp - datetime.timedelta(minutes=TIME_TO_START),
                #         user.strip(),
                #         not user.strip()
                #     )
                #     if user.strip():
                #         print(
                #             booking_data.loc[
                #                 currently_booked_or_printing_rows[user], "Status"
                #             ]
                #             == booking_statuses[USER_BOOKED]
                #         )

                if printer.gcode_state in ["RUNNING", "PAUSE"]:
                    # if printer is currently printing

                    user = status_data.loc[i, "Current User"]

                    if (
                        status_data.loc[i, "Status"]
                        == printer_statuses[PRINTER_PRINTING]
                    ) and (
                        (
                            datetime.datetime.strptime(
                                status_data.loc[i, "Start Time"].strip() + ":00",
                                "%Y-%m-%d %H:%M:%S",
                            )
                            <= timestamp - datetime.timedelta(minutes=TIME_TO_START)
                            and (
                                not user.strip()
                                or booking_data.loc[
                                    currently_booked_or_printing_rows[user], "Status"
                                ]
                                == booking_statuses[USER_BOOKED]
                            )
                        )
                        or (printer.tool_temp_target > MAX_TOOL_TEMP)
                    ):
                        # if printer has been printing for more than 10 minutes and no user is recorded, or they didn't submit a start form, or the tool temp is too high
                        # TODO: cancel print
                        # TODO: send email
                        print("cancel!")
                        status_data.loc[i, "Status"] = printer_statuses[
                            PRINTER_CANCEL_PENDING
                        ]

                    if (
                        status_data.loc[i, "Status"]
                        == printer_statuses[PRINTER_AVAILABLE]
                    ):
                        print_without_booking.append(printer_name)
                        print_without_booking_data[printer_name] = (
                            i,
                            datetime.datetime.fromtimestamp(printer.start_time * 60),
                        )
                    elif (
                        status_data.loc[i, "Status"] == printer_statuses[PRINTER_BOOKED]
                    ):
                        print_with_booking.append(printer_name)
                        print_with_booking_data[printer_name] = (
                            i,
                            user,
                            currently_booked_or_printing_rows[user],
                            datetime.datetime.fromtimestamp(printer.start_time * 60),
                        )

                    update_printer_status(
                        i,
                        (
                            PRINTER_PRINTING
                            if status_data.loc[i, "Status"]
                            != printer_statuses[PRINTER_CANCEL_PENDING]
                            else None
                        ),
                        user,
                        datetime.datetime.fromtimestamp(printer.start_time * 60),
                        timestamp + datetime.timedelta(minutes=printer.time_remaining),
                    )
                elif status_data.loc[i, "Status"] == printer_statuses[PRINTER_PRINTING]:
                    # if printer just finished printing
                    if status_data.loc[i, "Current User"].strip():
                        complete_prints.append(status_data.loc[i, "Current User"])
                    update_printer_status(i, PRINTER_AVAILABLE, "", "", "")
                elif status_data.loc[i, "Status"] not in printer_statuses:
                    # if printer is not printing but no valid status is recorded
                    update_printer_status(i, PRINTER_AVAILABLE, "", "", "")

                # if printer is available and someone is waiting for it
                if (
                    status_data.loc[i, "Status"] == printer_statuses[PRINTER_AVAILABLE]
                    and waiting_for_printer
                ):
                    start_time = timestamp
                    if start_time.hour >= 21:
                        start_time = datetime.datetime.combine(
                            timestamp.date(), datetime.datetime.min.time()
                        ) + datetime.timedelta(days=1, hours=12)
                    end_time = start_time + datetime.timedelta(hours=BOOKING_TIME)
                    if end_time.hour >= 21:
                        # 3 hours + 12 hours for next day from 9pm to 12pm
                        end_time = end_time + datetime.timedelta(hours=3 + 12)

                    user = waiting_for_printer.pop(0)
                    row = waiting_for_printer_rows.pop(user)
                    update_printer_status(
                        i,
                        PRINTER_BOOKED,
                        user,
                        start_time,
                        end_time,
                    )
                    currently_booked_or_printing.append(user)
                    currently_booked_or_printing_rows[user] = row
                    booking_data.loc[row, "Status"] = booking_statuses[USER_BOOKED]
                    # TODO: send email to user
                    print("booked!")
                elif status_data.loc[i, "Status"] == printer_statuses[
                    PRINTER_BOOKED
                ] and timestamp >= datetime.datetime.strptime(
                    status_data.loc[i, "End Time"].strip() + ":00", "%Y-%m-%d %H:%M:%S"
                ):
                    # if printer is booked but booking time has expired
                    user = status_data.loc[i, "Current User"]
                    row = currently_booked_or_printing_rows.pop(user)
                    update_printer_status(i, PRINTER_AVAILABLE, "", "", "")
                    booking_data.loc[row, "Status"] = booking_statuses[USER_NO_START]

            for i in starting_data.index.values[::-1]:
                # print(starting_data.loc[i])
                if datetime.datetime.strptime(
                    starting_data.loc[i, "Timestamp"], "%Y-%m-%d %H:%M:%S"
                ) <= timestamp - datetime.timedelta(minutes=TIME_TO_START):
                    break
                if starting_data.loc[i, "Handled"] == "TRUE":
                    continue
                cruzid = starting_data.loc[i, "Email Address"].split("@")[0].strip()
                printer = starting_data.loc[i, "Printer"]

                print(printer, cruzid, print_without_booking, print_with_booking)

                if printer in print_without_booking and sheet.is_staff(cruzid=cruzid):
                    printer_num, start_time = print_without_booking_data[printer]
                    update_printer_status(printer_num, None, cruzid, None, None)
                    print_without_booking.remove(printer)
                    print_without_booking_data.pop(printer)
                    starting_data.loc[i, "Handled"] = "TRUE"
                elif printer in print_with_booking:
                    printer_num, user, row, start_time = print_with_booking_data[
                        printer
                    ]
                    if cruzid == user.strip():
                        booking_data.loc[row, "Status"] = booking_statuses[
                            USER_PRINTING
                        ]
                        starting_data.loc[i, "Handled"] = "TRUE"
                    elif sheet.is_staff(cruzid=cruzid):
                        booking_data.loc[row, "Status"] = booking_statuses[
                            USER_SUPERVISED
                        ]
                        print_with_booking.remove(printer)
                        print_with_booking_data.pop(printer)
                        starting_data.loc[i, "Handled"] = "TRUE"

            found_first_active_index = False

            print("complete", complete_prints)
            print("current", currently_booked_or_printing)
            print("current_rows", currently_booked_or_printing_rows)

            for i, row in booking_data.iloc[booking_index:].iterrows():
                cruzid = row["Email Address"].split("@")[0]
                if row["Status"] in ["", booking_statuses[USER_WAITING]]:
                    if sheet.is_staff(cruzid=cruzid) or sheet.get_access(
                        "3D Printing", cruzid=cruzid
                    ):
                        row["Status"] = booking_statuses[USER_WAITING]
                        if (
                            cruzid not in currently_booked_or_printing
                            and cruzid not in waiting_for_printer
                        ):
                            waiting_for_printer.append(cruzid)
                            waiting_for_printer_rows[cruzid] = i
                            # TODO: send email to user
                            print("waiting!")
                    else:
                        row["Status"] = booking_statuses[USER_NOT_CERTIFIED]
                elif (
                    row["Status"] == booking_statuses[USER_PRINTING]
                    and cruzid in complete_prints
                ):
                    row["Status"] = booking_statuses[USER_DONE]
                    complete_prints.remove(cruzid)
                    currently_booked_or_printing.remove(cruzid)
                    currently_booked_or_printing_rows.pop(cruzid)

                if (
                    not found_first_active_index
                    and row["Status"]
                    in booking_statuses[USER_WAITING : (USER_PRINTING + 1)]
                ):
                    found_first_active_index = True
                    booking_index = i

            print("waiting", waiting_for_printer)
            print("waiting_rows", waiting_for_printer_rows)

            print()
            write_booking_sheet()
            write_starting_sheet()
            write_status_sheet()

            time.sleep(10)
    except KeyboardInterrupt:
        exit(0)
