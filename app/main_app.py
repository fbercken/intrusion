import os, sys, time, threading, pathlib, signal
import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib


Gst.init(None)

# ---------- config ----------
CAMERAS = [                                    # (cam_id, uri) — sharded across pods
    (0, "rtsp://admin:pass@192.168.1.64:554/stream"),
    (1, "rtsp://admin:pass@192.168.1.65:554/stream"),
]

SEG_DIR   = pathlib.Path("/tmp/segments"); 
SEG_DIR.mkdir(exist_ok=True)
KAFKA_CONN = os.environ["KAFKA_CONN"]          # "broker:9092;detections"
PGIE_CFG  = "/app/pgie_yolo26n_config.txt"


class Camera:

    def __init__(self, mux, id: int, uri: str):
        self.cameraID = id
        self.cameraURI = uri
        self.mux = mux

        src = Gst.ElementFactory.make("uridecodebin", f"src{self.cameraID}")
        src.set_property("uri", self.cameraURI)
        src.set_property("drop-on-latency", True)
        src.connect("pad-added", self.on_decode_pad)
        return src

    def  make_splitmuxsink(self):
        sink = Gst.ElementFactory.make("splitmuxsink", f"rec{self.cameraID}")
        sink.set_property("max-size-time", 300 * Gst.SECOND)   # rotate every 5 min
        sink.set_property("async-finalize", True)
        sink.set_property("location", str(SEG_DIR / f"cam{self.cameraID}_seg%05d.mp4"))
        sink.set_property("muxer-factory", "mp4mux")
        return sink

    def on_decode_pad(self, pad):
        caps, s = pad.get_current_caps(), pad.get_name()
        if s.startswith("video") and not pad.is_linked():
            sink_pad = self.mux.get_request_pad(f"sink_{self.cameraID}")
            pad.link(sink_pad)

    def on_demux_pad(self, pad, rec_sinks):
        # demux emits pads in source order: pad_0 -> cam0, etc.
        pad.link(rec_sinks[self.cameraID].get_static_pad("sink"))


# ---------- Pipeline definition ----------
def build():
    pipe = Gst.Pipeline()
    mux = Gst.ElementFactory.make("nvstreammux", "mux")
    mux.set_property("batch-size", len(CAMERAS))
    mux.set_property("width", 1280)
    mux.set_property("height", 720)
    mux.set_property("batched-push-timeout", 40000)
    pipe.add(mux)

    for i, (_,uri) in enumerate(CAMERAS):
        camera = Camera( mux, i, uri)
        pipe.add(camera)


    pgie = Gst.ElementFactory.make("nvinfer", "pgie")
    pgie.set_property("config-file-path", PGIE_CFG)
    tee = Gst.ElementFactory.make("tee", "tee")

    q_msg = Gst.ElementFactory.make("queue", "q_msg")
    msgconv = Gst.ElementFactory.make("nvmsgconv", "msgconv")
    msgconv.set_property("payload-type", 0)                 # DeepStream schema (sensor.id, bbox, confidence)
    
    msgbroker = Gst.ElementFactory.make("nvmsgbroker", "broker")
    msgbroker.set_property("proto-lib", "/opt/nvidia/deepstream/deepstream/lib/libnvds_kafka_proto.so")
    msgbroker.set_property("conn-str", KAFKA_CONN)
    msgbroker.set_property("sync", False)

    q_vid = Gst.ElementFactory.make("queue", "q_vid")
    osd = Gst.ElementFactory.make("nvdsosd", "osd")
    demux = Gst.ElementFactory.make("nvstreamdemux", "demux")

    rec_sinks = [make_splitmuxsink(cam_id) for cam_id, _ in CAMERAS]

    for el in [pgie, tee, q_msg, msgconv, msgbroker, q_vid, osd, demux, *rec_sinks]:
        pipe.add(el)

    mux.link(pgie); pgie.link(tee)
    tee.link(q_msg); q_msg.link(msgconv); msgconv.link(msgbroker)      # detections → Kafka
    tee.link(q_vid); q_vid.link(osd); osd.link(demux)                  # annotated video → record
    demux.connect("pad-added", on_demux_pad, rec_sinks)
    return pipe


def main():
    pipe = build()
    loop = GLib.MainLoop()
    
    def shutdown(*_):
        pipe.send_event(Gst.Event.new_eos())          # finalize current mp4 segments
        GLib.timeout_add(3000, loop.quit)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    pipe.set_state(Gst.State.PLAYING)
    print(f"[deepstream] streaming {len(CAMERAS)} cameras")

    try:
        loop.run()
    finally:
        pipe.set_state(Gst.State.NULL)                # ensures splitmuxsink writes trailer


main()
