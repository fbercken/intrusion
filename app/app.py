import os
import cv2
import json
import asyncio
from ultralytics import YOLO
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse


class Event(BaseModel):
    eventType: str
    source:  str | None
    data: str | None

app = FastAPI()

# Mount static files directory for CSS
#app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

model = YOLO('yolo26n.pt') 

@app.get("/test")
async def test():
    return {"message": "Hello World"}

@app.post("/controller/")
async def controller(event: Event):
    print(event)
    return event




def getFrames(source):
    """Generator capturing video frames, running YOLOv8, and streaming MJPEG."""
    fps = 20
    width = 640
    height = 480

    cap = cv2.VideoCapture(0, cv2.CAP_AVFOUNDATION)

    codec = cv2.VideoWriter.fourcc(*"XVID")
    out = cv2.VideoWriter('./processed.avi' , codec, fps, (width, height))


    while cap.isOpened():
        success, frame = cap.read()
        if not success:
            break
        
        # Run YOLOv8 inference for person/intrusion detection
        CONF_THRESHOLD=0.75
        results = model.predict(frame, conf=CONF_THRESHOLD, verbose=False)

        for result in results:
            for box in result.boxes:
                x1,y1,x2,y2 = map(int, box.xyxy[0])
                cls = results[0].names[int(box.cls[0])]
                conf = float(box.conf[0])
                crop = frame[y1:y2,x1:x2]
                cv2.rectangle(frame,(x1,y1),(x2,y2), (0.255,0),2)

   
                #print(  conf ) 

        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        out.write(hsv)


        #annotated_frame = results[0].plot()
    
        # Encode frame as JPEG
        ret, buffer = cv2.imencode('.jpg',  frame)
        if not ret:
            continue
            
        frame_bytes = buffer.tobytes()
        
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
    cap.release()


@app.get("/video_feed")
def live_stream():
    """Streams live annotated frames from the selected camera source."""
    source = "rtsp://localhost:5000/"
    if source is None:
        raise HTTPException(status_code=404, detail="Camera source not found in topology.")
    return StreamingResponse(getFrames(source), media_type="multipart/x-mixed-replace; boundary=frame")


@app.get("/",response_class=HTMLResponse)
def serve_homepage(request: Request):
    index_path = "templates/index.html"
    if os.path.exists(index_path):
        with open(index_path, "r") as f:
            return HTMLResponse(content=f.read())
    return templates.TemplateResponse(request,"index.html", { "request": request})
   # return HTMLResponse(content="<h3>Dashboard template missing</h3>", status_code=404)