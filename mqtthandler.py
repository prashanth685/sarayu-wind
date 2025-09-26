import paho.mqtt.client as mqtt
from PyQt5.QtCore import QObject, pyqtSignal, QTimer
import struct
import re
import json
import logging
from datetime import datetime
import threading
import queue
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class MQTTHandler(QObject):
    # feature_name, tag_name, model_name, channel_name (or None), values, sample_rate, frame_index
    data_received = pyqtSignal(str, str, str, object, list, int, int)
    connection_status = pyqtSignal(str)
    save_status = pyqtSignal(str)
    # model_name, tag_name, gap_voltages (list of floats)
    gap_values_received = pyqtSignal(str, str, list)

    def __init__(self, db, project_name, broker="192.168.1.231", port=1883):
        super().__init__()
        self.db = db
        self.project_name = project_name
        self.broker = broker
        self.port = port
        self.client = None
        self.connected = False
        self.subscribed_topics = []
        self.data_queue = queue.Queue()
        # Base batch interval, will adapt based on queue load
        self.batch_interval_ms = 80
        self.min_interval_ms = 40
        self.max_interval_ms = 200
        self.processing_thread = None
        self.running = False
        self.channel_counts = {}
        self.saving_filenames = {}
        self.active_features = defaultdict(lambda: defaultdict(set))  # feature_name -> model_name -> set(channels or None)
        self.feature_mapping = {
            "Tabular View": ["TabularView"],
            "Time View": ["TimeWave", "TimeReport"],
            "Time Report": ["TimeReport"],
            "FFT": ["FFT"],
            "Waterfall": ["WaterFall"],
            "Centerline": ["CenterLinePlot"],
            "Orbit": ["OrbitView"],
            "Trend View": ["TrendView"],
            "Multiple Trend View": ["MultiTrendView"],
            "Bode Plot": ["BodePlot"],
            "History Plot": ["HistoryPlot"],
            "Polar Plot": ["PolarPlot"],
            "Report": ["Report"]
        }
        # Features that must receive all channels together
        self.all_channels_features = [
            "Time View",
            "Time Report",
            "Tabular View",
            "Trend View",
            "Multiple Trend View",
            "Waterfall",
            "Orbit",
            "Bode Plot",
            "Centerline"
        ]
        logging.debug(f"Initializing MQTTHandler with project_name: {project_name}, broker: {broker}")

    def add_active_feature(self, feature_name, model_name, channel=None):
        if feature_name in self.feature_mapping:
            self.active_features[feature_name][model_name].add(channel)
            logging.debug(f"Added active feature: {feature_name}/{model_name}/{channel or 'None'}")

    def remove_active_feature(self, feature_name, model_name, channel=None):
        if feature_name in self.feature_mapping:
            if model_name in self.active_features[feature_name]:
                self.active_features[feature_name][model_name].discard(channel)
                if not self.active_features[feature_name][model_name]:
                    del self.active_features[feature_name][model_name]
                if not self.active_features[feature_name]:
                    del self.active_features[feature_name]
                logging.debug(f"Removed active feature: {feature_name}/{model_name}/{channel or 'None'}")

    def start_saving(self, model_name, filename):
        self.saving_filenames[model_name] = filename
        logging.info(f"Started saving for model {model_name} to {filename}")

    def stop_saving(self, model_name):
        if model_name in self.saving_filenames:
            del self.saving_filenames[model_name]
            logging.info(f"Stopped saving for model {model_name}")

    def parse_topic(self, topic):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            tag_name = topic
            project_data = self.db.get_project_data(self.project_name)
            if not project_data or "models" not in project_data:
                logging.error(f"No valid project data for {self.project_name}")
                return None, None, None
            model_name = None
            for model in project_data["models"]:
                if model.get("tagName") == topic:
                    model_name = model.get("name")
                    break
            if not model_name:
                logging.warning(f"No model found for topic {topic} in project {self.project_name}")
                return None, None, None
            channel_count_map = {"DAQ4CH": 4, "DAQ8CH": 8, "DAQ10CH": 10}
            raw_channel_count = project_data.get("channel_count", 4)
            try:
                if isinstance(raw_channel_count, str):
                    raw_norm = str(raw_channel_count).strip().upper().replace(" ", "").replace("_", "")
                    channel_count = channel_count_map.get(raw_norm)
                    if channel_count is None:
                        m = re.search(r"(\d+)", raw_norm)
                        if m:
                            channel_count = int(m.group(1))
                        else:
                            channel_count = int(raw_norm)
                else:
                    channel_count = int(raw_channel_count)
                if channel_count not in [4, 8, 10]:
                    raise ValueError(f"Invalid channel count: {channel_count}")
            except (ValueError, TypeError) as e:
                logging.error(f"Invalid channel count {raw_channel_count}: {str(e)}. Defaulting to 4.")
                channel_count = 4
            self.channel_counts[self.project_name] = channel_count
            logging.debug(f"Parsed topic {topic}: project_name={self.project_name}, model_name={model_name}, tag_name={tag_name}, channels={channel_count}")
            return self.project_name, model_name, tag_name
        except Exception as e:
            logging.error(f"Error parsing topic {topic}: {str(e)}")
            return None, None, None

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            self.connection_status.emit("Connected to MQTT Broker")
            logging.info("Connected to MQTT Broker")
            QTimer.singleShot(0, self.subscribe_to_topics)
        else:
            self.connected = False
            self.connection_status.emit(f"Connection failed with code {rc}")
            logging.error(f"Failed to connect to MQTT Broker with code {rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        self.connection_status.emit("Disconnected from MQTT Broker")
        logging.info("Disconnected from MQTT Broker")
        self.subscribed_topics = []

    def on_message(self, client, userdata, msg):
        try:
            self.data_queue.put((msg.topic, msg.payload, datetime.now()))
            # Avoid chatty debug logs per message in hot path
        except Exception as e:
            logging.error(f"Error queuing MQTT message: {str(e)}")

    def process_data(self):
        while self.running:
            try:
                try:
                    topic, payload, timestamp = self.data_queue.get(timeout=self.batch_interval_ms / 1000.0)
                except queue.Empty:
                    # Lightly relax interval when idle
                    self.batch_interval_ms = max(self.min_interval_ms, int(self.batch_interval_ms * 0.9))
                    continue

                # Drain the queue quickly and keep only the latest message (coalescing)
                drained = 0
                while True:
                    try:
                        topic, payload, timestamp = self.data_queue.get_nowait()
                        drained += 1
                    except queue.Empty:
                        break

                # Adapt interval based on backlog processed
                if drained > 5:
                    self.batch_interval_ms = min(self.max_interval_ms, int(self.batch_interval_ms * 1.2))
                elif drained == 0:
                    self.batch_interval_ms = max(self.min_interval_ms, int(self.batch_interval_ms * 0.95))

                project_name, model_name, tag_name = self.parse_topic(topic)
                if not tag_name or project_name != self.project_name or not model_name:
                    logging.warning(f"Skipping invalid topic: {topic}")
                    continue

                channel_count = self.channel_counts.get(self.project_name, 4)
                project_data = self.db.get_project_data(self.project_name)
                model = next((m for m in project_data["models"] if m["name"] == model_name), None)
                if not model:
                    logging.error(f"Model {model_name} not found")
                    continue
                # Prepare actual channel names from the model for main channels
                channel_names = []
                try:
                    channel_names = [ch.get("channelName") for ch in model.get("channels", [])]
                except Exception:
                    channel_names = []

                try:
                    values = None
                    sample_rate = 1000
                    frame_index = 0
                    main_channels = channel_count
                    tacho_channels_count = 2
                    samples_per_channel = 0
                    try:
                        payload_str = payload.decode('utf-8')
                        data = json.loads(payload_str)
                        values = data.get("values", [])
                        sample_rate = data.get("sample_rate", 1000)
                        frame_index = data.get("frame_index", 0)
                        main_channels = data.get("main_channels", channel_count)
                        tacho_channels_count = data.get("tacho_channels", 2)
                        samples_per_channel = len(values[0]) if values and len(values) > 0 else 0
                        if not isinstance(values, list) or len(values) < main_channels:
                            logging.warning(f"Invalid JSON payload format or insufficient channels: {len(values)}/{main_channels}")
                            continue
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        payload_length = len(payload)
                        if payload_length < 20 or payload_length % 2 != 0:
                            logging.warning(f"Invalid payload length: {payload_length} bytes")
                            continue

                        num_samples = payload_length // 2
                        try:
                            values = struct.unpack(f"<{num_samples}H", payload)
                        except struct.error as e:
                            logging.error(f"Failed to unpack payload of {num_samples} uint16_t: {str(e)}")
                            continue

                        if len(values) < 100:
                            logging.warning(f"Payload too short: {len(values)} samples")
                            continue

                        header = values[:100]
                        frame_index = (header[1] << 16) | header[0]
                        main_channels = header[2] if len(header) > 2 else channel_count
                        sample_rate = header[3] if len(header) > 3 else 1000
                        tacho_channels_count = header[6] if len(header) > 6 else 2
                        total_channels = main_channels + tacho_channels_count
                        total_values = values[100:]
                        samples_per_channel = (len(total_values) // total_channels) if total_values and total_channels > 0 else 0

                        # Extract gap voltages from header[15]..header[25] and scale by 1/100
                        try:
                            if len(header) >= 26:
                                gaps = [float(h) / 100.0 for h in header[15:26]]
                                # Emit asynchronously for interested features (e.g., Tabular View)
                                self.gap_values_received.emit(model_name, tag_name, gaps)
                        except Exception:
                            # Do not fail processing on gap extraction issues
                            pass

                        if main_channels <= 0 or sample_rate <= 0 or tacho_channels_count < 0 or samples_per_channel <= 0:
                            logging.error(f"Invalid header: main_channels={main_channels}, sample_rate={sample_rate}, "
                                          f"tacho_channels_count={tacho_channels_count}, samples_per_channel={samples_per_channel}")
                            continue

                        if len(total_values) != samples_per_channel * total_channels:
                            logging.warning(f"Unexpected data length: got {len(total_values)}, expected {samples_per_channel * total_channels}")
                            continue

                        channel_data = [[] for _ in range(main_channels)]
                        if main_channels == 4:
                            main_data = total_values[:samples_per_channel * main_channels]
                            for i in range(0, len(main_data), 4):
                                for ch in range(4):
                                    if i + ch < len(main_data):
                                        channel_data[ch].append(main_data[i + ch])
                        elif main_channels == 10:
                            adc1_data = total_values[:samples_per_channel * 6]
                            adc2_data = total_values[samples_per_channel * 6:samples_per_channel * 10]
                            for i in range(0, len(adc1_data), 6):
                                for ch in range(6):
                                    if i + ch < len(adc1_data):
                                        channel_data[ch].append(adc1_data[i + ch])
                            for i in range(0, len(adc2_data), 4):
                                for ch in range(4):
                                    if i + ch < len(adc2_data):
                                        channel_data[ch + 6].append(adc2_data[i + ch])
                        else:
                            main_data = total_values[:samples_per_channel * main_channels]
                            for i in range(0, len(main_data), main_channels):
                                for ch in range(main_channels):
                                    if i + ch < len(main_data):
                                        channel_data[ch].append(main_data[i + ch])

                        tacho_data = total_values[samples_per_channel * main_channels:]
                        tacho_freq_data = tacho_data[:samples_per_channel] if tacho_channels_count >= 1 else []
                        tacho_trigger_data = tacho_data[samples_per_channel:2 * samples_per_channel] if tacho_channels_count >= 2 else []
                        values = [[float(v) for v in ch] for ch in channel_data]
                        if tacho_freq_data:
                            values.append([float(v) for v in tacho_freq_data])
                        if tacho_trigger_data:
                            values.append([float(v) for v in tacho_trigger_data])

                    if not values or len(values) == 0:
                        logging.warning(f"No valid data extracted from payload for topic {topic}")
                        continue

                    if model_name in self.saving_filenames:
                        filename = self.saving_filenames[model_name]
                        flattened_message = []
                        for ch in range(main_channels):
                            flattened_message.extend(values[ch])
                        if tacho_channels_count >= 1:
                            flattened_message.extend(values[main_channels])
                        if tacho_channels_count >= 2:
                            flattened_message.extend(values[main_channels + 1])

                        message_data = {
                            "topic": tag_name,
                            "filename": filename,
                            "frameIndex": frame_index,
                            "message": flattened_message,
                            "numberOfChannels": main_channels,
                            "samplingRate": sample_rate,
                            "samplingSize": samples_per_channel,
                            "messageFrequency": None,
                            "tacoChannelCount": tacho_channels_count,
                            "createdAt": datetime.now().isoformat(),
                            "updatedAt": datetime.now().isoformat()
                        }
                        success, msg = self.db.save_history_message(self.project_name, model_name, message_data)
                        if success:
                            logging.info(f"Saved data to database: {filename}, frame {frame_index}")
                            self.save_status.emit(f"Saved data to {filename}, frame {frame_index}")
                        else:
                            logging.error(f"Failed to save history message: {msg}")
                            self.save_status.emit(f"Failed to save history message: {msg}")

                    for feature_name in list(self.active_features.keys()):
                        if model_name in self.active_features[feature_name]:
                            active_channels = self.active_features[feature_name][model_name]
                            if feature_name in self.all_channels_features:
                                if None in active_channels or active_channels:
                                    # channel_name=None indicates all-channel payload
                                    self.data_received.emit(feature_name, tag_name, model_name, None, values, sample_rate, frame_index)
                                    # Reduce debug noise in hot path
                            else:
                                for ch_idx in range(len(values)):
                                    # Use actual channel names for main channels
                                    if ch_idx < main_channels and ch_idx < len(channel_names) and channel_names[ch_idx]:
                                        ch_key = channel_names[ch_idx]
                                    else:
                                        # Fallback to tacho naming for non-main channels
                                        tacho_idx = ch_idx - main_channels + 1
                                        ch_key = f"Tacho_{tacho_idx}"

                                    if ch_key in active_channels or None in active_channels:
                                        # Emit per-channel payload with channel name
                                        self.data_received.emit(feature_name, tag_name, model_name, ch_key, values[ch_idx], sample_rate, frame_index)

                except Exception as e:
                    logging.error(f"Error processing payload for topic {topic}: {str(e)}")

            except Exception as e:
                logging.error(f"Error in data processing loop: {str(e)}")
                self.connection_status.emit(f"Data processing error: {str(e)}")

    def subscribe_to_topics(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.project_name)
            for model in project_data.get("models", []):
                tag_name = model.get("tagName", "")
                if tag_name and tag_name not in self.subscribed_topics:
                    self.client.subscribe(tag_name)
                    self.subscribed_topics.append(tag_name)
                    logging.info(f"Subscribed to topic: {tag_name}")
        except Exception as e:
            logging.error(f"Error subscribing to topics: {str(e)}")
            self.connection_status.emit(f"Failed to subscribe to topics: {str(e)}")

    def start(self):
        try:
            self.client = mqtt.Client()
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_message = self.on_message
            self.client.connect_async(self.broker, self.port, 60)
            self.client.loop_start()
            self.running = True
            self.processing_thread = threading.Thread(target=self.process_data, daemon=True)
            self.processing_thread.start()
            logging.info("MQTT client and processing thread started")
        except Exception as e:
            logging.error(f"Failed to start MQTT client: {str(e)}")
            self.connection_status.emit(f"Failed to start MQTT: {str(e)}")

    def stop(self):
        try:
            self.running = False
            if self.processing_thread:
                self.processing_thread.join(timeout=1.0)
                self.processing_thread = None
            if self.client:
                self.client.loop_stop()
                self.client.disconnect()
                self.connected = False
                self.subscribed_topics = []
                logging.info("MQTT client and processing thread stopped")
        except Exception as e:
            logging.error(f"Error stopping MQTT client: {str(e)}")