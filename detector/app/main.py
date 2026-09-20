import os
import sys
import time
import threading
import pathlib
import signal
import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

# ---------- Configuration ----------
SEG_DIR = pathlib.Path("/tmp/videos")
SEG_DIR.mkdir(exist_ok=True)
PGIE_CFG = "/app/pgie_yolo26n_config.txt"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","video-event")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","datafabric01.ezmeral.demo.hpelabs.fr:9092")


username = os.getenv("KAFKA_SASL_USERNAME","mapr")
password = os.getenv("KAFKA_SASL_PASSWORD","mapr")

config_content = f"""[message-broker]
consumer-group-id = deepstream_consumer_group
proto-cfg = "security.protocol=sasl_plaintext;sasl.mechanisms=PLAIN;sasl.username={username};sasl.password={password}"
partition-key = sensorId
"""

# Write out the config file dynamically at runtime
config_path = "/tmp/cfg_kafka.txt"
with open(config_path, "w") as f:
    f.write(config_content)



class CameraStream:
    """Encapsulates an individual camera source and its dedicated recording sink."""
    def __init__(self, manager, cam_id: int, uri: str):
        self.cam_id = cam_id
        self.uri = uri
        self.manager = manager
        self.pipeline = manager.pipeline

        # Create a Bin to group camera elements cleanly
        self.bin = Gst.Bin.new(f"cam_bin_{self.cam_id}")

        # Source element (uridecodebin)
        self.source = Gst.ElementFactory.make("uridecodebin", f"src_{self.cam_id}")
        self.source.set_property("uri", self.uri)
        self.source.set_property("drop-on-latency", True)
        self.source.connect("pad-added", self.on_decode_pad)

        # Recording sink element (splitmuxsink)
        self.sink = Gst.ElementFactory.make("splitmuxsink", f"rec_{self.cam_id}")
        self.sink.set_property("max-size-time", 300 * Gst.SECOND)  # rotate every 5 min
        self.sink.set_property("async-finalize", True)
        self.sink.set_property("location", str(SEG_DIR / f"cam{self.cam_id}_seg%05d.mp4"))
        self.sink.set_property("muxer-factory", "mp4mux")

        self.bin.add(self.source)
        self.bin.add(self.sink)
        
        # Add bin to main pipeline and sync state
        self.pipeline.add(self.bin)
        self.bin.sync_state_with_parent()

    def on_decode_pad(self, element, pad):
        caps = pad.get_current_caps()
        name = pad.get_name()
        if caps and "video" in caps.to_string() and not pad.is_linked():
            # Request sink pad from nvstreammux
            mux_pad_name = f"sink_{self.cam_id}"
            sink_pad = self.manager.mux.get_request_pad(mux_pad_name)
            if sink_pad:
                pad.link(sink_pad)

    def link_demux(self, demux_pad):
        """Links the demultiplexer output pad for this camera to its splitmuxsink."""
        sink_pad = self.sink.get_static_pad("sink")
        if sink_pad and not demux_pad.is_linked():
            demux_pad.link(sink_pad)

    def destroy(self):
        """Safely tears down and removes camera elements from the pipeline."""
        self.source.set_state(Gst.State.NULL)
        self.sink.set_state(Gst.State.NULL)
        self.bin.set_state(Gst.State.NULL)
        self.pipeline.remove(self.bin)

        # Release mux pad
        mux_pad_name = f"sink_{self.cam_id}"
        sink_pad = self.manager.mux.get_static_pad(mux_pad_name)
        if sink_pad:
            self.manager.mux.release_request_pad(sink_pad)


class DeepStreamPipelineManager:
    """Manages the main DeepStream pipeline, inference, Kafka publishing, and dynamic camera streams."""
    def __init__(self, initial_cameras=None):
        self.pipeline = Gst.Pipeline.new("main-pipeline")
        self.cameras = {}  # cam_id -> CameraStream

        # Core pipeline elements
        self.mux = Gst.ElementFactory.make("nvstreammux", "mux")
        self.mux.set_property("width", 1280)
        self.mux.set_property("height", 720)
        self.mux.set_property("batched-push-timeout", 40000)

        self.pgie = Gst.ElementFactory.make("nvinfer", "pgie")
        self.pgie.set_property("config-file-path", PGIE_CFG)

        self.tee = Gst.ElementFactory.make("tee", "tee")

        # Kafka branch
        self.q_msg = Gst.ElementFactory.make("queue", "q_msg")
        self.msgconv = Gst.ElementFactory.make("nvmsgconv", "msgconv")
        self.msgconv.set_property("payload-type", 0)
        
        self.msgbroker = Gst.ElementFactory.make("nvmsgbroker", "broker")
        self.msgbroker.set_property("proto-lib", "/opt/nvidia/deepstream/deepstream/lib/libnvds_kafka_proto.so")
        self.msgbroker.set_property("conn-str", KAFKA_BOOTSTRAP_SERVERS)
        self.msgbroker.set_property("config", "/tmp/cfg_kafka.txt")
        self.msgbroker.set_property("topic", KAFKA_TOPIC)
        self.msgbroker.set_property("sync", False)

        # Video / OSD / Demux branch
        self.q_vid = Gst.ElementFactory.make("queue", "q_vid")
        self.osd = Gst.ElementFactory.make("nvdsosd", "osd")
        self.demux = Gst.ElementFactory.make("nvstreamdemux", "demux")

        # Live Display branch (real-time preview window)
        self.live_queue = Gst.ElementFactory.make("queue", "live_q")
        self.live_convert = Gst.ElementFactory.make("nvvidconv", "live_conv")
        self.live_sink = Gst.ElementFactory.make("autovideosink", "live_sink")

        # Add core elements
        core_elements = [
            self.mux, self.pgie, self.tee, 
            self.q_msg, self.msgconv, self.msgbroker, 
            self.q_vid, self.osd, self.demux,
            self.live_queue, self.live_convert, self.live_sink
        ]
        for el in core_elements:
            self.pipeline.add(el)

        # Link static core elements
        self.mux.link(self.pgie)
        self.pgie.link(self.tee)

        self.tee.link(self.q_msg)
        self.q_msg.link(self.msgconv)
        self.msgconv.link(self.msgbroker)

        self.tee.link(self.q_vid)
        self.q_vid.link(self.osd)
        self.osd.link(self.demux)

        # Link live display branch
        self.live_queue.link(self.live_convert)
        self.live_convert.link(self.live_sink)

        # Demux pad listener
        self.demux.connect("pad-added", self.on_demux_pad)

        # Initialize cameras
        initial_cams = initial_cameras or []
        self.mux.set_property("batch-size", max(1, len(initial_cams)))
        for cam_id, uri in initial_cams:
            self.add_camera(cam_id, uri)

    def on_demux_pad(self, demux, pad):
        pad_name = pad.get_name()
        try:
            cam_id = int(pad_name.split("_")[-1])
            if cam_id in self.cameras:
                self.cameras[cam_id].link_demux(pad)
        except Exception as e:
            print(f"[pipeline] Error linking demux pad {pad_name}: {e}")

    def add_camera(self, cam_id: int, uri: str):
        if cam_id in self.cameras:
            print(f"[pipeline] Camera {cam_id} already exists.")
            return

        print(f"[pipeline] Dynamically adding Camera {cam_id}: {uri}")
        is_playing = self.pipeline.get_state(0).state == Gst.State.PLAYING
        if is_playing:
            self.pipeline.set_state(Gst.State.READY)

        self.mux.set_property("batch-size", len(self.cameras) + 1)
        cam_stream = CameraStream(self, cam_id, uri)
        self.cameras[cam_id] = cam_stream

        if is_playing:
            self.pipeline.set_state(Gst.State.PLAYING)

    def remove_camera(self, cam_id: int):
        if cam_id not in self.cameras:
            print(f"[pipeline] Camera {cam_id} not found.")
            return

        print(f"[pipeline] Dynamically removing Camera {cam_id}")
        is_playing = self.pipeline.get_state(0).state == Gst.State.PLAYING
        if is_playing:
            self.pipeline.set_state(Gst.State.READY)

        cam_stream = self.cameras.pop(cam_id)
        cam_stream.destroy()

        if len(self.cameras) > 0:
            self.mux.set_property("batch-size", len(self.cameras))

        if is_playing:
            self.pipeline.set_state(Gst.State.PLAYING)

    def select_live_display(self, cam_id: int):
        """Switches the live video display feed in real time to the chosen camera."""
        if cam_id not in self.cameras:
            print(f"[display] Camera {cam_id} is not active. Cannot display.")
            return

        print(f"[display] Switching real-time preview to Camera {cam_id}")
        
        # Unlink current live queue source if linked
        sink_pad = self.live_queue.get_static_pad("sink")
        if sink_pad.is_linked():
            peer = sink_pad.get_peer()
            if peer:
                peer.get_parent_element().unlink(self.live_queue)

        # Link demux source pad for this camera to the live display queue
        demux_pad_name = f"src_{cam_id}"
        demux_src_pad = self.demux.get_static_pad(demux_pad_name)
        if demux_src_pad:
            demux_src_pad.link(sink_pad)
        else:
            print(f"[display] Warning: Demux pad for camera {cam_id} not ready yet.")

    def start(self):
        self.pipeline.set_state(Gst.State.PLAYING)
        print(f"[deepstream] Pipeline running with active cameras: {list(self.cameras.keys())}")

    def stop(self):
        self.pipeline.send_event(Gst.Event.new_eos())
        time.sleep(1)
        self.pipeline.set_state(Gst.State.NULL)
        print("[deepstream] Pipeline stopped.")


def interactive_cli(manager):
    """CLI loop allowing dynamic camera management and live view selection at runtime."""
    time.sleep(2)  # Let pipeline initialize
    while True:
        print("\n--- Camera Manager Menu ---")
        print("1. Add Camera")
        print("2. Remove Camera")
        print("3. Select Live Display Camera")
        print("4. List Active Cameras")
        print("5. Exit")
        choice = input("Select an option: ").strip()

        if choice == "1":
            try:
                cid = int(input("Enter Camera ID (int): "))
                uri = input("Enter RTSP URI: ").strip()
                manager.add_camera(cid, uri)
            except ValueError:
                print("Invalid ID format.")
        elif choice == "2":
            try:
                cid = int(input("Enter Camera ID to remove: "))
                manager.remove_camera(cid)
            except ValueError:
                print("Invalid ID format.")
        elif choice == "3":
            try:
                cid = int(input("Enter Camera ID to display live: "))
                manager.select_live_display(cid)
            except ValueError:
                print("Invalid ID format.")
        elif choice == "4":
            print(f"Active Cameras: {list(manager.cameras.keys())}")
        elif choice == "5":
            break


def main():
    initial_cameras = [
        (0, "rtsp://admin:pass@192.168.1.64:554/stream"),
        (1, "rtsp://admin:pass@192.168.1.65:554/stream"),
    ]

    manager = DeepStreamPipelineManager(initial_cameras)
    manager.start()

    # Start interactive management thread
    cli_thread = threading.Thread(target=interactive_cli, args=(manager,), daemon=True)
    cli_thread.start()

    loop = GLib.MainLoop()

    def shutdown(*_):
        manager.stop()
        loop.quit()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        loop.run()
    except Exception as e:
        print(f"Error in main loop: {e}")
    finally:
        manager.stop()


if __name__ == "__main__":
    main()