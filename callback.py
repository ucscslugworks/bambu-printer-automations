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
        # get data from Access Card sheet (3D printer access, staff members)
        sheet.get_sheet_data(False)
        # get data from printer automations sheet (bookings, startings, statuses, limits, logs)
        get_sheet_data()

        # set up printers
        for name in printer_data:
            # get printer config from json file
            p = printer_data[name]
            # check if all required fields are present
            if (
                "hostname" not in p
                or "access_code" not in p
                or "serial_number" not in p
            ):
                print(
                    f"Error: printer config for {name} missing hostname, access_code, or serial_number"
                )
                exit(1)

            # create printer config object using IP, access code, and serial number
            config = BambuConfig(
                hostname=p["hostname"],
                access_code=p["access_code"],
                serial_number=p["serial_number"],
            )
            # create printer object using config
            printer = BambuPrinter(config=config)
            # add printer to list of printers
            printers.append((name, printer))
            # start session with printer
            printer.start_session()

        # check if number of printers in printers.json matches number of printers in status sheet
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
        )  # data for printers that have started prints with a booking - printer num, user (from status_data), row number in booking_data, start time

        while True:
            try:
                # get data from Access Card sheet (3D printer access, staff members)
                sheet.get_sheet_data(False)
                # get data from printer automations sheet (bookings, startings, statuses, limits, logs)
                get_sheet_data()
                # get current timestamp to be used for calculations
                timestamp = datetime.datetime.now()

                # list of users who have completed their prints and their row numbers
                complete_prints = []

                for i, (printer_name, printer) in enumerate(printers):
                    print(f"Printer {i}: {printer_name}")

                    # if printer._lastMessageTime:
                    #     print(
                    #         f"last checkin: {round(time.time() - printer._lastMessageTime)}s ago"
                    #     )
                    # print(f"print=[{printer.gcode_state}]")
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
                    print(status_data.loc[i])
                    print()

                    if printer.gcode_state in ["RUNNING", "PAUSE"]:
                        # if printer is currently printing

                        # get user who booked/started the print (could be blank)
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
                                        currently_booked_or_printing_rows[user],
                                        "Status",
                                    ]
                                    == booking_statuses[USER_BOOKED]
                                )
                            )
                            or (
                                printer.tool_temp_target > MAX_TOOL_TEMP
                                and not sheet.is_staff(cruzid=user.strip())
                            )
                            # or (not limits_data.empty and not sheet.is_staff(cruzid=user.strip() and limits_data.loc[user.strip(), "Limits (grams)"])
                        ):
                            # if printer has been printing for more than 10 minutes and no user is recorded, or they didn't submit a start form, or the tool temp is too high (and they're not staff)
                            # TODO: cancel print
                            # TODO: send email
                            # TODO: log cancelation
                            print("cancel!")
                            status_data.loc[i, "Status"] = printer_statuses[
                                PRINTER_CANCEL_PENDING
                            ]

                        if (
                            status_data.loc[i, "Status"]
                            == printer_statuses[PRINTER_AVAILABLE]
                        ):
                            # if printer is printing but status was set to available
                            # a print must have been started without a booking
                            print_without_booking.append(printer_name)
                            print_without_booking_data[printer_name] = (
                                i,
                                datetime.datetime.fromtimestamp(
                                    printer.start_time * 60
                                ),
                            )
                        elif (
                            status_data.loc[i, "Status"]
                            == printer_statuses[PRINTER_BOOKED]
                        ):
                            # if printer is printing but status was set to booked
                            # a print must have been started with a booking (but may not have been started by the user who booked it)
                            print_with_booking.append(printer_name)
                            print_with_booking_data[printer_name] = (
                                i,
                                user,
                                currently_booked_or_printing_rows[user],
                                datetime.datetime.fromtimestamp(
                                    printer.start_time * 60
                                ),
                            )

                        # update printer status in status sheet
                        update_printer_status(
                            i,  # printer number
                            (
                                PRINTER_PRINTING
                                if status_data.loc[i, "Status"]
                                != printer_statuses[PRINTER_CANCEL_PENDING]
                                else None
                            ),  # set status to printing, unless it is cancel pending (in which case leave it)
                            None,  # do not change the user
                            datetime.datetime.fromtimestamp(
                                printer.start_time * 60
                            ),  # set start time to the time the print started
                            timestamp
                            + datetime.timedelta(
                                minutes=printer.time_remaining
                            ),  # set end time to the time the print will finish
                        )
                    elif (
                        status_data.loc[i, "Status"]
                        == printer_statuses[PRINTER_PRINTING]
                    ):
                        # if printer just finished printing
                        # get user who booked/started the print
                        user = status_data.loc[i, "Current User"].strip()
                        # if user is currently printing
                        if user and user in currently_booked_or_printing:
                            # add user to list of completed prints
                            complete_prints.append(user)
                        # set printer status to available
                        update_printer_status(i, PRINTER_AVAILABLE, "", "", "")
                    elif (
                        status_data.loc[i, "Status"] not in printer_statuses
                        or status_data.loc[i, "Status"]
                        == printer_statuses[PRINTER_CANCEL_PENDING]
                    ):
                        # if printer is not printing but no valid status is recorded, or if print was canceled
                        # set printer status to available
                        update_printer_status(i, PRINTER_AVAILABLE, "", "", "")

                    # if printer is available and someone is waiting for a printer
                    if (
                        status_data.loc[i, "Status"]
                        == printer_statuses[PRINTER_AVAILABLE]
                        and waiting_for_printer
                    ):
                        # get start time for booking
                        start_time = timestamp

                        if start_time.hour >= 21:
                            # if the start time is after 9pm, set start time to 12pm the next day
                            start_time = datetime.datetime.combine(
                                timestamp.date(), datetime.datetime.min.time()
                            ) + datetime.timedelta(days=1, hours=12)
                        elif start_time.hour < 12:
                            # if the start time is before 12pm, set start time to 12pm
                            start_time = datetime.datetime.combine(
                                timestamp.date(), datetime.datetime.min.time()
                            ) + datetime.timedelta(hours=12)

                        # get end time for booking
                        end_time = start_time + datetime.timedelta(hours=BOOKING_TIME)
                        if end_time.hour >= 21:
                            # 3 hours + 12 hours for next day from 9pm to 12pm
                            end_time = end_time + datetime.timedelta(hours=3 + 12)

                        # get first user waiting for a printer
                        user = waiting_for_printer.pop(0)
                        # get row number of user in booking_data
                        row = waiting_for_printer_rows.pop(user)
                        # update printer status in status sheet
                        update_printer_status(
                            i,  # printer number
                            PRINTER_BOOKED,  # set status to booked
                            user,  # set user to the user who booked the printer
                            start_time,
                            end_time,
                        )
                        # add user to list of currently booked or printing users
                        currently_booked_or_printing.append(user)
                        currently_booked_or_printing_rows[user] = row
                        # update booking status in booking sheet
                        booking_data.loc[row, "Status"] = booking_statuses[USER_BOOKED]
                        # TODO: send email to user
                        # TODO: log booking
                        print("booked!")
                    elif status_data.loc[i, "Status"] == printer_statuses[
                        PRINTER_BOOKED
                    ] and timestamp >= datetime.datetime.strptime(
                        status_data.loc[i, "End Time"].strip() + ":00",
                        "%Y-%m-%d %H:%M:%S",
                    ):
                        # if printer is booked but booking time has expired
                        # get user who booked the printer
                        user = status_data.loc[i, "Current User"]
                        row = currently_booked_or_printing_rows.pop(user)
                        # update printer status in status sheet to available
                        update_printer_status(i, PRINTER_AVAILABLE, "", "", "")
                        # update booking status in booking sheet to did not start print
                        booking_data.loc[row, "Status"] = booking_statuses[
                            USER_NO_START
                        ]

                print(status_data.loc[i])
                print()

                for i in starting_data.index.values[::-1]:
                    # iterate through starting data in reverse order

                    if datetime.datetime.strptime(
                        starting_data.loc[i, "Timestamp"], "%Y-%m-%d %H:%M:%S"
                    ) <= timestamp - datetime.timedelta(minutes=TIME_TO_START):
                        # if the starting data is more than 10 minutes old, stop iterating
                        break

                    if starting_data.loc[i, "Handled"] == "TRUE":
                        # if the starting data has already been handled, skip
                        continue

                    # get cruzid and printer number from starting data
                    cruzid = starting_data.loc[i, "Email Address"].split("@")[0].strip()
                    printer = starting_data.loc[i, "Printer"]

                    # print(printer, cruzid, print_without_booking, print_with_booking)

                    if printer in print_without_booking and sheet.is_staff(
                        cruzid=cruzid
                    ):
                        # if printer has started a print without a booking and the user is staff
                        printer_num, start_time = print_without_booking_data[printer]
                        # update printer status in status sheet to add the user
                        update_printer_status(printer_num, None, cruzid, None, None)
                        # remove printer from list of printers that have started prints without a booking
                        print_without_booking.remove(printer)
                        print_without_booking_data.pop(printer)
                        # update starting data to show that it has been handled
                        starting_data.loc[i, "Handled"] = "TRUE"
                    elif printer in print_with_booking:
                        # if printer has started a print with a booking
                        # get the print data
                        printer_num, user, row, start_time = print_with_booking_data[
                            printer
                        ]
                        if cruzid == user.strip():
                            # if the user who booked the printer is the one from the start form
                            # update booking status in booking sheet to currently printing
                            booking_data.loc[row, "Status"] = booking_statuses[
                                USER_PRINTING
                            ]
                            # remove printer from list of printers that have started prints with a booking
                            print_with_booking.remove(printer)
                            print_with_booking_data.pop(printer)
                            # set starting data to handled
                            starting_data.loc[i, "Handled"] = "TRUE"
                            # TODO: log print
                        elif sheet.is_staff(cruzid=cruzid):
                            # if the user who started the print is staff
                            # update booking status in booking sheet to supervised printing
                            booking_data.loc[row, "Status"] = booking_statuses[
                                USER_SUPERVISED
                            ]
                            # remove printer from list of printers that have started prints with a booking
                            print_with_booking.remove(printer)
                            print_with_booking_data.pop(printer)
                            # set starting data to handled
                            starting_data.loc[i, "Handled"] = "TRUE"
                            # TODO: log print

                # boolean to check if the first active index has been found
                found_first_active_index = False

                # print("complete", complete_prints)
                # print("current", currently_booked_or_printing)
                # print("current_rows", currently_booked_or_printing_rows)

                for i, row in booking_data.iloc[booking_index:].iterrows():
                    # iterate through booking data starting from the first active index
                    # get cruzid from email address
                    cruzid = row["Email Address"].split("@")[0]
                    if row["Status"] in ["", booking_statuses[USER_WAITING]]:
                        # if the booking status is blank or waiting for printer
                        if sheet.is_staff(cruzid=cruzid) or sheet.get_access(
                            "3D Printing", cruzid=cruzid
                        ):
                            # if the user is staff or has access to 3D printing
                            # set the user to waiting for printer
                            row["Status"] = booking_statuses[USER_WAITING]
                            if (
                                cruzid not in currently_booked_or_printing
                                and cruzid not in waiting_for_printer
                            ):
                                # if the user is not currently booked or printing and not already waiting for a printer
                                # add the user to the list of users waiting for a printer & save the row number
                                waiting_for_printer.append(cruzid)
                                waiting_for_printer_rows[cruzid] = i
                                # TODO: send email to user
                                # TODO: log addition to queue
                                print("waiting!")
                        else:
                            # if the user is not staff and does not have access to 3D printing
                            # set the user to not certified
                            row["Status"] = booking_statuses[USER_NOT_CERTIFIED]
                    elif (
                        row["Status"]
                        in [
                            booking_statuses[USER_PRINTING],
                            booking_statuses[USER_SUPERVISED],
                        ]
                        and cruzid in complete_prints
                        and i == currently_booked_or_printing_rows[cruzid]
                    ):
                        # if the user is currently printing and has completed their print (and this is their booking row)
                        # set the user to print done
                        row["Status"] = booking_statuses[USER_DONE]
                        # remove the user from the list of users who have completed their prints and the list of currently booked or printing users
                        complete_prints.remove(cruzid)
                        currently_booked_or_printing.remove(cruzid)
                        currently_booked_or_printing_rows.pop(cruzid)

                    if (
                        not found_first_active_index
                        and row["Status"]
                        in booking_statuses[USER_WAITING : (USER_SUPERVISED + 1)]
                    ):
                        # if the first active index has not been found and the booking status is one of the active statuses
                        # set the first active index to the current index
                        found_first_active_index = True
                        booking_index = i

                # print("waiting", waiting_for_printer)
                # print("waiting_rows", waiting_for_printer_rows)

                # print()
                # write data to sheets
                write_booking_sheet()
                write_starting_sheet()
                write_status_sheet()

                while datetime.datetime.now() < timestamp + datetime.timedelta(
                    seconds=10
                ):
                    time.sleep(1)

            except Exception as e:
                if type(e) == KeyboardInterrupt:
                    raise e
                print("Error")

    except KeyboardInterrupt:
        print("Exiting...")
        for _, printer in printers:
            printer.quit()
        exit(0)
