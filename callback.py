import json
import os
import sys
import time

from bpm.bambuconfig import BambuConfig
from bpm.bambuprinter import BambuPrinter
from bpm.bambutools import parseFan, parseStage

printer_data = json.load(open("printers.json"))
printers = []

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


def on_update(printer):

    print(
        f"tool=[{round(printer.tool_temp, 1)}/{round(printer.tool_temp_target, 1)}] "
        + f"bed=[{round(printer.bed_temp, 1)}/{round(printer.bed_temp_target, 1)}] "
        + f"fan=[{parseFan(printer.fan_speed)}] print=[{printer.gcode_state}] speed=[{printer.speed_level}] "
        + f"light=[{'on' if printer.light_state else 'off'}]"
    )

    print(
        f"stg_cur=[{parseStage(printer.current_stage)}] file=[{printer.gcode_file}] "
        + f"layers=[{printer.layer_count}] layer=[{printer.current_layer}] "
        + f"%=[{printer.percent_complete}] eta=[{printer.time_remaining} min] "
        + f"spool=[{printer.active_spool} ({printer.spool_state})]"
    )


for p, printer in printers:
    printer.start_session()

while True:
    for printer_name, printer in printers:
        print(f"Printer: {printer_name}")

        if printer._lastMessageTime:
            print(f"last checkin: {round(time.time() - printer._lastMessageTime)}s ago")

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

    print()

    time.sleep(1)
