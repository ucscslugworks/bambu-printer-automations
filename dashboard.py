import json
import sys

import cv2
from flask import Flask, Response, render_template

# https://github.com/akmamun/multiple-camera-stream


app = Flask(__name__)


cameras = []
printer_data = json.load(open("printers.json"))

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
        sys.exit(1)
    cameras.append(f"rtsps://bblp:{p["access_code"]}@{p["hostname"]}/streaming/live/1")



def find_camera(id):
    return cameras[int(id)]


def gen_frames(camera_id):

    cam = find_camera(camera_id)
    cap = cv2.VideoCapture(cam)

    while True:
        success, frame = cap.read()  # read the camera frame
        if success:
            _, buffer = cv2.imencode(".jpg", frame)
            frame = buffer.tobytes()
            out = (
                b"--frame\r\n" b"Content-Type: image/jpeg\r\n\r\n" + frame + b"\r\n"
            )  # concat frame one by one and show result
# if LIVESTREAM:
            # yield out
# else:
            return out
        else:
            break


@app.route("/video_feed/<string:id>/", methods=["GET"])
def video_feed(id):
    """Video streaming route. Put this in the src attribute of an img tag."""
    return Response(
        gen_frames(id), mimetype="multipart/x-mixed-replace; boundary=frame"
    )


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", camera_count=len(cameras))


if __name__ == "__main__":
    app.run()
