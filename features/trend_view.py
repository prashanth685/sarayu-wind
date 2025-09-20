from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt5.QtCore import Qt
import pyqtgraph as pg
import numpy as np
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TimeAxisItem(pg.AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def tickStrings(self, values, scale, spacing):
        return [datetime.fromtimestamp(val).strftime('%H:%M:%S') for val in values]

class TrendViewFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None, channel_count=None):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.model_name = model_name
        self.console = console
        self.scaling_factor = 3.3 / 65535.0
        self.display_window_seconds = 60.0
        self.channel_name = channel
        self.channel_count = int(channel_count) if channel_count is not None else self.get_channel_count_from_db()
        self.channel = self.resolve_channel_index(channel) if channel is not None else None
        self.sample_rate = None
        self.plot_data = []
        self.user_interacted = False
        self.last_right_limit = None
        self.last_frame_index = -1
        self.widget = None
        self.initUI()
        if self.console:
            self.console.append_to_console(
                f"Initialized TrendViewFeature for {self.model_name or 'No Model'}/{self.channel_name or 'No Channel'} "
                f"with {self.channel_count} channels"
            )

    def get_channel_count_from_db(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.project_name)
            if not project_data:
                if self.console:
                    self.console.append_to_console(f"Project {self.project_name} not found in database")
                return 1
            model = next((m for m in project_data.get("models", []) if m["name"] == self.model_name), None)
            if not model:
                if self.console:
                    self.console.append_to_console(f"Model {self.model_name} not found")
                return 1
            channels = model.get("channels", [])
            return max(1, len(channels))
        except Exception as e:
            if self.console:
                self.console.append_to_console(f"Error retrieving channel count from database: {str(e)}")
            logging.error(f"Error retrieving channel count from database: {str(e)}")
            return 1

    def resolve_channel_index(self, channel):
        try:
            if isinstance(channel, str):
                project_data = self.db.get_project_data(self.project_name) if self.db else {}
                models = project_data.get("models", [])
                for m_data in models:
                    if m_data.get("name") == self.model_name:
                        channels = m_data.get("channels", [])
                        for idx, ch in enumerate(channels):
                            if ch.get("channelName") == channel:
                                logging.debug(f"Resolved channel {channel} to index {idx} in model {self.model_name}")
                                return idx
                        logging.warning(f"Channel {channel} not found in model {self.model_name}. Available channels: {[ch.get('channelName') for ch in channels]}")
                        if self.console:
                            self.console.append_to_console(f"Warning: Channel {channel} not found in model {self.model_name}")
                        return None
                logging.warning(f"Model {self.model_name} not found in project {self.project_name}")
                if self.console:
                    self.console.append_to_console(f"Warning: Model {self.model_name} not found in project {self.project_name}")
                return None
            elif isinstance(channel, int):
                if channel >= 0:
                    return channel
                else:
                    logging.warning(f"Invalid channel index: {channel}")
                    if self.console:
                        self.console.append_to_console(f"Warning: Invalid channel index: {channel}")
                    return None
            else:
                logging.warning(f"Invalid channel type: {type(channel)}")
                if self.console:
                    self.console.append_to_console(f"Warning: Invalid channel type: {type(channel)}")
                return None
        except Exception as e:
            logging.error(f"Failed to resolve channel index for {channel}: {e}")
            if self.console:
                self.console.append_to_console(f"Error: Failed to resolve channel index for {channel}: {e}")
            return None

    def initUI(self):
        self.widget = QWidget()
        layout = QVBoxLayout()
        self.widget.setLayout(layout)

        display_channel = self.channel_name if self.channel_name else f"Channel_{self.channel + 1}" if self.channel is not None else "Unknown"
        self.label = QLabel(f"Trend View for Model: {self.model_name or 'Unknown'}, Channel: {display_channel}")
        layout.addWidget(self.label)

        self.plot_widget = pg.PlotWidget(axisItems={'bottom': TimeAxisItem(orientation='bottom')})
        self.plot_widget.setTitle(f"Trend for {self.model_name or 'Unknown'} - {display_channel}")
        self.plot_widget.setLabel('left', 'Direct (Peak-to-Peak Voltage, V)')
        self.plot_widget.setLabel('bottom', 'Time (hh:mm:ss)')
        self.plot_widget.showGrid(x=True, y=True)
        self.plot_widget.setBackground('w')
        self.plot_widget.setXRange(-self.display_window_seconds, 0, padding=0.02)
        self.plot_widget.enableAutoRange('y', True)
        layout.addWidget(self.plot_widget)

        self.curve = self.plot_widget.plot(pen=pg.mkPen('b', width=1))
        try:
            self.curve.setDownsampling(auto=True)
            self.curve.setClipToView(True)
        except Exception:
            pass
        self.curve.setSymbol('o')
        self.curve.setSymbolSize(5)

        self.plot_widget.scene().sigMouseClicked.connect(self.on_mouse_interaction)
        self.plot_widget.getViewBox().sigRangeChangedManually.connect(self.on_range_changed)

    def on_mouse_interaction(self, event):
        self.user_interacted = True

    def on_range_changed(self, view_box, ranges):
        self.user_interacted = True
        self.last_right_limit = ranges[0][1]

    def get_widget(self):
        return self.widget

    def on_data_received(self, tag_name, model_name, values, sample_rate, frame_index):
        if self.model_name != model_name or self.channel is None:
            if self.console:
                self.console.append_to_console(
                    f"TrendView: Skipped data - model_name={model_name} (expected {self.model_name}), "
                    f"channel_index={self.channel}, frame {frame_index}"
                )
            return

        try:
            if frame_index != self.last_frame_index + 1 and self.last_frame_index != -1:
                logging.warning(f"Non-sequential frame index: expected {self.last_frame_index + 1}, got {frame_index}")
                if self.console:
                    self.console.append_to_console(f"Warning: Non-sequential frame index: expected {self.last_frame_index + 1}, got {frame_index}")
            self.last_frame_index = frame_index

            # Dynamically handle values format: full channels or per channel
            if len(values) == 0:
                logging.warning(f"Empty values for frame {frame_index}")
                if self.console:
                    self.console.append_to_console(f"TrendView: Empty values for frame {frame_index}")
                return

            if isinstance(values[0], (list, np.ndarray)):
                # Full channels mode
                total_channels = len(values)
                if total_channels < self.channel_count:
                    if self.console:
                        self.console.append_to_console(
                            f"TrendView: Received {total_channels} channels, expected at least {self.channel_count}, frame {frame_index}"
                        )
                    return
                if self.channel >= total_channels:
                    if self.console:
                        self.console.append_to_console(
                            f"TrendView: Channel index {self.channel} out of range for {total_channels} channels, frame {frame_index}"
                        )
                    return
                channel_data = values[self.channel]
                # Assume last channel is trigger if available
                trigger_data = values[-1] if total_channels >= 2 else np.zeros_like(channel_data)
            else:
                # Per channel mode
                if self.channel is not None:
                    if self.console:
                        self.console.append_to_console(
                            f"TrendView: Received per-channel data, but channel index {self.channel} specified, skipping frame {frame_index}"
                        )
                    return
                channel_data = values
                trigger_data = np.zeros_like(channel_data)  # No trigger in per-channel mode

            self.sample_rate = sample_rate if sample_rate > 0 else 1000
            channel_data = np.array(channel_data, dtype=np.float32) * self.scaling_factor
            trigger_data = np.array(trigger_data, dtype=np.float32)

            trigger_indices = np.where(trigger_data == 1)[0].tolist()
            min_distance_between_triggers = 5
            filtered_trigger_indices = [trigger_indices[0]] if trigger_indices else []
            for i in range(1, len(trigger_indices)):
                if trigger_indices[i] - filtered_trigger_indices[-1] >= min_distance_between_triggers:
                    filtered_trigger_indices.append(trigger_indices[i])

            if len(filtered_trigger_indices) < 2:
                # Fallback: compute peak-to-peak over entire window
                logging.warning(f"Not enough trigger points detected, using window p2p fallback, frame {frame_index}")
                if self.console:
                    self.console.append_to_console(f"TrendView: Not enough trigger points, using window p2p fallback (frame {frame_index})")
                direct_values = [float(np.max(channel_data) - np.min(channel_data))]
            else:
                direct_values = []
                for i in range(len(filtered_trigger_indices) - 1):
                    start_idx = filtered_trigger_indices[i]
                    end_idx = filtered_trigger_indices[i + 1]
                    if end_idx <= start_idx:
                        continue
                    segment_data = channel_data[start_idx:end_idx]
                    if len(segment_data) == 0:
                        continue
                    peak_to_peak = segment_data.max() - segment_data.min()
                    direct_values.append(peak_to_peak)

            if not direct_values:
                # Secondary fallback safety
                logging.warning(f"No valid segments for calculation, using window p2p fallback, frame {frame_index}")
                if self.console:
                    self.console.append_to_console(f"TrendView: No valid segments, using window p2p fallback (frame {frame_index})")
                direct_values = [float(np.max(channel_data) - np.min(channel_data))]

            direct_average = np.mean(direct_values)
            timestamp = datetime.now().timestamp()
            self.plot_data.append((timestamp, direct_average))

            self.trim_old_data()
            self.update_plot()

            logging.debug(f"Processed TrendView for {tag_name}, Channel {self.channel_name or self.channel}: Direct value {direct_average:.4f} at {datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}, frame {frame_index}")
            if self.console:
                self.console.append_to_console(f"TrendView {tag_name}: Direct={direct_average:.4f} V at {datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}, frame {frame_index}")

        except Exception as e:
            logging.error(f"TrendView: Data processing error for channel {self.channel_name or self.channel}, frame {frame_index}: {e}")
            if self.console:
                self.console.append_to_console(f"TrendView: Data processing error for channel {self.channel_name or self.channel}, frame {frame_index}: {e}")

    def load_selected_frame(self, payload: dict):
        try:
            if not payload:
                if self.console:
                    self.console.append_to_console("TrendView: Invalid selection payload (empty).")
                return
            num_main = int(payload.get("numberOfChannels", 0))
            num_tacho = int(payload.get("tacoChannelCount", 0))
            total_ch = num_main + num_tacho
            Fs = float(payload.get("samplingRate", 0) or 0)
            N = int(payload.get("samplingSize", 0) or 0)
            data_flat = payload.get("message", [])
            if not Fs or not N or not total_ch or not data_flat:
                if self.console:
                    self.console.append_to_console("TrendView: Incomplete selection payload (Fs/N/channels/data missing).")
                return

            # Shape data into channels if flattened
            if isinstance(data_flat, list) and data_flat and isinstance(data_flat[0], (int, float)):
                if len(data_flat) != total_ch * N:
                    if self.console:
                        self.console.append_to_console(f"TrendView: Data length mismatch. Expected {total_ch*N}, got {len(data_flat)}")
                    return
                values = []
                for ch in range(total_ch):
                    start = ch * N
                    end = start + N
                    values.append(data_flat[start:end])
            else:
                # Assume already list-of-lists
                values = data_flat
                if len(values) != total_ch or any(len(v) != N for v in values):
                    if self.console:
                        self.console.append_to_console("TrendView: Invalid nested data shape in selection payload.")
                    return

            # Update channel count dynamically if mismatched
            if num_main != self.channel_count:
                if self.console:
                    self.console.append_to_console(f"TrendView: Adjusting channel count from {self.channel_count} to {num_main} based on payload.")
                self.channel_count = num_main

            # Default to first channel if none selected
            channel_idx = self.channel if self.channel is not None else 0
            if channel_idx >= num_main:
                if self.console:
                    self.console.append_to_console(f"TrendView: Channel index {channel_idx} out of range for {num_main} main channels, defaulting to 0")
                channel_idx = 0

            self.sample_rate = Fs
            channel_data = np.array(values[channel_idx], dtype=np.float32) * self.scaling_factor
            trigger_data = np.array(values[-1], dtype=np.float32) if total_ch >= 2 else np.zeros_like(channel_data)

            trigger_indices = np.where(trigger_data == 1)[0].tolist()
            min_distance_between_triggers = 5
            filtered_trigger_indices = [trigger_indices[0]] if trigger_indices else []
            for i in range(1, len(trigger_indices)):
                if trigger_indices[i] - filtered_trigger_indices[-1] >= min_distance_between_triggers:
                    filtered_trigger_indices.append(trigger_indices[i])

            if len(filtered_trigger_indices) < 2:
                # Fallback: compute peak-to-peak over entire window
                if self.console:
                    self.console.append_to_console(f"TrendView: Not enough triggers in selected frame, using window p2p fallback")
                direct_values = [float(np.max(channel_data) - np.min(channel_data))]
            else:
                direct_values = []
                for i in range(len(filtered_trigger_indices) - 1):
                    start_idx = filtered_trigger_indices[i]
                    end_idx = filtered_trigger_indices[i + 1]
                    if end_idx <= start_idx:
                        continue
                    segment_data = channel_data[start_idx:end_idx]
                    if len(segment_data) == 0:
                        continue
                    peak_to_peak = segment_data.max() - segment_data.min()
                    direct_values.append(peak_to_peak)

            if not direct_values:
                if self.console:
                    self.console.append_to_console(f"TrendView: No valid segments in selected frame, using window p2p fallback")
                direct_values = [float(np.max(channel_data) - np.min(channel_data))]

            direct_average = np.mean(direct_values)
            timestamp = datetime.now().timestamp()
            self.plot_data = [(timestamp, direct_average)]  # Replace with single frame data
            self.trim_old_data()
            self.update_plot()

            if self.console:
                self.console.append_to_console(
                    f"TrendView: Loaded selected frame {payload.get('frameIndex')} ({N} samples @ {Fs}Hz), "
                    f"Direct={direct_average:.4f} V at {datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')}"
                )

        except Exception as e:
            if self.console:
                self.console.append_to_console(f"TrendView: Error loading selected frame: {str(e)}")
            logging.error(f"TrendView: Error loading selected frame: {str(e)}")

    def trim_old_data(self):
        now = datetime.now().timestamp()
        self.plot_data = [(t, v) for t, v in self.plot_data if (now - t) <= self.display_window_seconds]

    def update_plot(self):
        if not self.plot_data:
            self.curve.clear()
            return

        timestamps, voltages = zip(*self.plot_data)
        timestamps = np.array(timestamps)
        voltages = np.array(voltages)

        if self.user_interacted and self.last_right_limit is not None:
            max_time = self.last_right_limit
            min_time = max_time - self.display_window_seconds
        else:
            max_time = timestamps.max() if len(timestamps) > 0 else datetime.now().timestamp()
            min_time = max_time - self.display_window_seconds
            if len(timestamps) > 0 and (timestamps.max() - timestamps.min()) < self.display_window_seconds:
                min_time = timestamps.min()

        plot_width = self.plot_widget.width() or 600
        total_span = max_time - min_time
        padding_time = (40.0 / plot_width) * total_span

        self.plot_widget.setXRange(min_time, max_time + padding_time, padding=0.0)
        if len(voltages) > 0:
            min_y = voltages.min() * 0.9
            max_y = voltages.max() * 1.1
            self.plot_widget.setYRange(min_y, max_y, padding=0.02)

        self.curve.setData(timestamps, voltages)