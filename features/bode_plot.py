import numpy as np
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QProgressBar
from PyQt5.QtCore import QTimer
import pyqtgraph as pg
from pymongo import MongoClient
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class BodePlotFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.selected_channel = channel
        self.model_name = model_name
        self.console = console
        self.widget = None
        self.plot_widgets = {}
        self.plots = {}
        self.data = {}
        self.tag_name = None
        self.channel_names = []
        self.channel_indices = {}
        self.scaling_factor = 3.3 / 65535.0  # Voltage scaling for ADC
        self.colors = {
            'amplitude': (0, 0, 255),  # Blue
            'phase': (255, 0, 0)       # Red
        }
        self.init_data()
        self.init_ui()
        if hasattr(self.parent, 'channel_selected'):
            self.parent.channel_selected.connect(self.on_channel_selected)
            self.log_info("Connected to channel_selected signal")
        else:
            self.log_error("Parent does not have channel_selected signal")

    def init_data(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.project_name)
            if not project_data or "models" not in project_data:
                self.log_error(f"Project {self.project_name} or models not found.")
                return
            model = next((m for m in project_data["models"] if m["name"] == self.model_name), None)
            if not model or not model.get("tagName"):
                self.log_error(f"TagName not found for Model: {self.model_name}")
                return
            self.tag_name = model["tagName"]
            self.channel_names = [c["channelName"] for c in model.get("channels", [])]
            self.channel_indices = {name: idx for idx, name in enumerate(self.channel_names)}
            if not self.channel_names:
                self.log_error(f"No channels found in model {self.model_name}.")
                return
            for ch_name in self.channel_names:
                self.data[ch_name] = {
                    'frequencies': [],
                    'amplitudes': [],
                    'phases': []
                }
            self.log_info(f"Initialized BodePlotFeature for Model: {self.model_name}, Tag: {self.tag_name}, Channels: {self.channel_names}")
            if self.selected_channel and self.selected_channel in self.channel_names:
                self.log_info(f"Initial channel set to: {self.selected_channel}")
            else:
                self.selected_channel = self.channel_names[0] if self.channel_names else None
                self.log_info(f"Initial channel set to: {self.selected_channel}")
        except Exception as e:
            self.log_error(f"Error initializing BodePlotFeature: {str(e)}")

    def init_ui(self):
        self.widget = QWidget()
        main_layout = QVBoxLayout()
        self.widget.setLayout(main_layout)

        header_label = QLabel(f"Bode Plot for Model: {self.model_name}")
        header_label.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        main_layout.addWidget(header_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)

        self.plot_container = QWidget()
        self.plot_layout = QVBoxLayout()
        self.plot_container.setLayout(self.plot_layout)
        main_layout.addWidget(self.plot_container)

        self.error_label = QLabel("Waiting for data or select a channel...")
        self.error_label.setStyleSheet("color: red; font-size: 14px; padding: 10px;")
        self.error_label.setAlignment(pg.QtCore.Qt.AlignCenter)
        main_layout.addWidget(self.error_label)
        self.error_label.setVisible(True)

        for ch_name in self.channel_names:
            channel_widget = QWidget()
            channel_layout = QVBoxLayout()
            channel_widget.setLayout(channel_layout)
            channel_widget.setVisible(ch_name == self.selected_channel)

            # Amplitude Plot (Magnitude vs Frequency)
            amp_plot = pg.PlotWidget()
            amp_plot.setBackground('w')
            amp_plot.showGrid(x=True, y=True)
            amp_plot.setLabel('bottom', 'Frequency (Hz)')
            amp_plot.setLabel('left', 'Magnitude (dB)')
            amp_plot.setTitle(f"Magnitude vs Frequency - {ch_name}")
            amp_plot.addLegend()
            amp_plot.setLogMode(x=True, y=False)  # Logarithmic frequency axis
            amp_line = amp_plot.plot([], [], pen=pg.mkPen(color=self.colors['amplitude'], width=2), name=ch_name)
            self.plot_widgets[f"{ch_name}_amp"] = amp_plot
            self.plots[f"{ch_name}_amp"] = amp_line
            channel_layout.addWidget(amp_plot)

            # Phase Plot
            phase_plot = pg.PlotWidget()
            phase_plot.setBackground('w')
            phase_plot.showGrid(x=True, y=True)
            phase_plot.setLabel('bottom', 'Frequency (Hz)')
            phase_plot.setLabel('left', 'Phase (deg)')
            phase_plot.setTitle(f"Phase vs Frequency - {ch_name}")
            phase_plot.addLegend()
            phase_plot.setLogMode(x=True, y=False)  # Logarithmic frequency axis
            phase_line = phase_plot.plot([], [], pen=pg.mkPen(color=self.colors['phase'], width=2), name=ch_name)
            self.plot_widgets[f"{ch_name}_phase"] = phase_plot
            self.plots[f"{ch_name}_phase"] = phase_line
            channel_layout.addWidget(phase_plot)

            self.plot_widgets[f"{ch_name}_widget"] = channel_widget
            self.plot_layout.addWidget(channel_widget)

        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_plots)
        self.update_timer.start(1000)
        self.log_info("Initialized BodePlotFeature UI")

    def on_channel_selected(self, model_name, channel_name):
        if model_name != self.model_name:
            self.log_info(f"Ignoring channel selection for model: {model_name}")
            return
        if channel_name not in self.channel_names:
            self.log_error(f"Selected channel {channel_name} not found in model {model_name}")
            self.selected_channel = self.channel_names[0] if self.channel_names else None
            self.log_info(f"Defaulted to channel: {self.selected_channel}")
        else:
            self.selected_channel = channel_name
            self.log_info(f"Channel selected: {channel_name}")
        self.update_visible_plots()
        self.update_plots()

    def update_visible_plots(self):
        try:
            for ch_name in self.channel_names:
                visible = ch_name == self.selected_channel
                self.plot_widgets[f"{ch_name}_widget"].setVisible(visible)
                self.log_info(f"Set visibility for {ch_name}_widget: {visible}")
            if self.selected_channel:
                self.error_label.setVisible(False)
            else:
                self.error_label.setText("Please select a channel")
                self.error_label.setVisible(True)
            self.log_info(f"Updated visible plots for channel: {self.selected_channel}")
        except Exception as e:
            self.log_error(f"Error updating visible plots: {str(e)}")

    def log_info(self, message):
        logging.info(message)
        if self.console:
            self.console.append_to_console(message)

    def log_error(self, message):
        logging.error(message)
        if self.console:
            self.console.append_to_console(message)
        self.error_label.setText(message)
        self.error_label.setVisible(True)

    def on_data_received(self, feature_name, tag_name, model_name, values, sample_rate, frame_index):
        if self.model_name != model_name or self.tag_name != tag_name or feature_name != "Bode Plot":
            self.log_info(f"Ignoring data for feature: {feature_name}, tag: {tag_name}, model: {model_name}")
            return
        try:
            self.log_info(f"Received data: {len(values)} channels, sample_rate: {sample_rate}, frame_index: {frame_index}, first channel length: {len(values[0]) if values else 0}")

            expected_channels = len(self.channel_names) + 2  # Main channels + freq + trigger
            if len(values) < expected_channels:
                self.log_error(f"Invalid data: expected at least {expected_channels} channels (including freq and trigger), got {len(values)}")
                return

            main_data = values[:len(self.channel_names)]
            freq_data = values[len(self.channel_names)]
            trigger_data = values[len(self.channel_names) + 1]
            self.log_info(f"Main data channels: {len(main_data)}, Freq data length: {len(freq_data)}, Trigger data length: {len(trigger_data)}")

            if not self.selected_channel:
                self.selected_channel = self.channel_names[0] if self.channel_names else None
                self.log_info(f"No channel selected; defaulted to {self.selected_channel}")

            if self.selected_channel:
                ch_idx = self.channel_indices.get(self.selected_channel)
                if ch_idx is not None and ch_idx < len(main_data):
                    channel_data = [float(v) * self.scaling_factor for v in main_data[ch_idx]]
                    self.process_data(channel_data, freq_data, trigger_data, self.selected_channel)
                else:
                    self.log_error(f"Invalid channel index {ch_idx} for {self.selected_channel}")
            else:
                self.log_error("No valid channel selected for processing")
                return

            self.update_plots()
        except Exception as e:
            self.log_error(f"Error processing data: {str(e)}")

    def process_data(self, channel_data, frequency_data, trigger_data, channel_name):
        try:
            if not channel_data or not frequency_data:
                self.log_error(f"Empty data for {channel_name}: channel_data={len(channel_data)}, frequency_data={len(frequency_data)}")
                return
            min_length = min(len(channel_data), len(frequency_data))
            if min_length < 1:
                self.log_error(f"Data too short for {channel_name}: length={min_length}")
                return
            channel_data = channel_data[:min_length]
            frequency_data = [f for f in frequency_data[:min_length] if f > 0]
            trigger_data = trigger_data[:min_length] if trigger_data else [0] * min_length
            if not frequency_data:
                self.log_error(f"No valid frequencies for {channel_name}")
                return
            min_length = len(frequency_data)
            channel_data = channel_data[:min_length]
            trigger_data = trigger_data[:min_length]
            self.log_info(f"Processing {min_length} samples for {channel_name}")
            self.log_info(f"Sample channel data: {channel_data[:5]}")
            self.log_info(f"Sample frequency data: {frequency_data[:5]}")
            self.log_info(f"Sample trigger data: {trigger_data[:5] if trigger_data else 'None'}")

            # Calculate amplitude (in dB) and phase
            amplitudes = []
            phases = []
            valid_freqs = []
            for v, f, t in zip(channel_data, frequency_data, trigger_data):
                if f <= 0:
                    continue
                amplitude = 20 * np.log10(abs(v)) if abs(v) > 0 else -100  # Convert to dB
                phase = np.angle(v, deg=True) if t != 0 else 0  # Use trigger for phase, fallback to 0
                if phase < 0:
                    phase += 360
                valid_freqs.append(f)
                amplitudes.append(amplitude)
                phases.append(phase)

            if not valid_freqs:
                self.log_error(f"No valid data points for {channel_name} after filtering")
                return

            # Sort data by frequency
            sorted_indices = np.argsort(valid_freqs)
            valid_freqs = [valid_freqs[i] for i in sorted_indices]
            amplitudes = [amplitudes[i] for i in sorted_indices]
            phases = [phases[i] for i in sorted_indices]

            # Smooth data using a moving average
            window_size = 5
            smoothed_freq = []
            smoothed_amp = []
            smoothed_phase = []
            for i in range(len(valid_freqs)):
                start_idx = max(0, i - window_size // 2)
                end_idx = min(len(valid_freqs), i + window_size // 2 + 1)
                window_freq = valid_freqs[start_idx:end_idx]
                window_amp = amplitudes[start_idx:end_idx]
                window_phase = phases[start_idx:end_idx]
                smoothed_freq.append(np.mean(window_freq))
                smoothed_amp.append(np.mean(window_amp))
                smoothed_phase.append(np.mean(window_phase))

            self.data[channel_name]['frequencies'] = smoothed_freq
            self.data[channel_name]['amplitudes'] = smoothed_amp
            self.data[channel_name]['phases'] = smoothed_phase
            self.log_info(f"Processed {len(smoothed_freq)} data points for {channel_name}: freq={smoothed_freq[:5]}, amp={smoothed_amp[:5]}, phase={smoothed_phase[:5]}")
        except Exception as e:
            self.log_error(f"Error processing data for {channel_name}: {str(e)}")

    def update_plots(self):
        try:
            if not self.selected_channel:
                self.error_label.setText("Please select a channel")
                self.error_label.setVisible(True)
                for ch_name in self.channel_names:
                    self.plots[f"{ch_name}_amp"].setData([], [])
                    self.plots[f"{ch_name}_phase"].setData([], [])
                self.log_info("No channel selected; cleared all plots")
                return

            ch_name = self.selected_channel
            freq = np.array(self.data[ch_name]['frequencies'], dtype=float)
            amp = np.array(self.data[ch_name]['amplitudes'], dtype=float)
            phase = np.array(self.data[ch_name]['phases'], dtype=float)
            self.log_info(f"Updating plots for {ch_name}: {len(freq)} data points, freq={freq[:5].tolist() if len(freq) > 0 else []}, amp={amp[:5].tolist() if len(amp) > 0 else []}, phase={phase[:5].tolist() if len(phase) > 0 else []}")

            self.update_visible_plots()

            if len(freq) == 0 or len(amp) == 0 or len(phase) == 0:
                self.plots[f"{ch_name}_amp"].setData([], [])
                self.plots[f"{ch_name}_phase"].setData([], [])
                self.error_label.setText(f"No valid data available for {ch_name}")
                self.error_label.setVisible(True)
                self.log_info(f"No valid data for {ch_name}: freq={len(freq)}, amp={len(amp)}, phase={len(phase)}")
                return

            if not (len(freq) == len(amp) == len(phase)):
                self.log_error(f"Data length mismatch for {ch_name}: freq={len(freq)}, amp={len(amp)}, phase={len(phase)}")
                self.error_label.setText(f"Data length mismatch for {ch_name}")
                self.error_label.setVisible(True)
                return

            # Update amplitude plot
            self.plots[f"{ch_name}_amp"].setData(freq, amp, connect="all")
            vb = self.plot_widgets[f"{ch_name}_amp"].getViewBox()
            x_min = min(freq) if len(freq) > 0 else 0.1
            x_max = max(freq) if len(freq) > 0 else 100
            y_min = min(amp) - 10 if len(amp) > 0 else -100
            y_max = max(amp) + 10 if len(amp) > 0 else 0
            vb.setXRange(np.log10(max(0.1, x_min)) - 0.1, np.log10(x_max) + 0.1)
            vb.setYRange(y_min, y_max)
            self.log_info(f"Updated amplitude plot for {ch_name}: x_range={x_min}-{x_max}, y_range={y_min}-{y_max}")

            # Update phase plot
            self.plots[f"{ch_name}_phase"].setData(freq, phase, connect="all")
            vb = self.plot_widgets[f"{ch_name}_phase"].getViewBox()
            x_min = min(freq) if len(freq) > 0 else 0.1
            x_max = max(freq) if len(freq) > 0 else 100
            y_min = max(-180, min(phase) - 10) if len(phase) > 0 else -180
            y_max = min(180, max(phase) + 10) if len(phase) > 0 else 180
            vb.setXRange(np.log10(max(0.1, x_min)) - 0.1, np.log10(x_max) + 0.1)
            vb.setYRange(y_min, y_max)
            self.log_info(f"Updated phase plot for {ch_name}: x_range={x_min}-{x_max}, y_range={y_min}-{y_max}")

            self.error_label.setVisible(False)
        except Exception as e:
            self.log_error(f"Error updating plots: {str(e)}")
            self.error_label.setText(f"Plotting error for {ch_name}: {str(e)}")
            self.error_label.setVisible(True)

    def process_historical_data(self, filename, frame_index):
        try:
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            client = MongoClient("mongodb://localhost:27017")
            database = client["changed_db"]
            history_collection = database["timeview_messages"]

            query = {
                "project_name": self.project_name,
                "model_name": self.model_name,
                "topic": self.tag_name,
                "filename": filename
            }
            total_frames = history_collection.count_documents(query)
            self.log_info(f"Found {total_frames} frames for filename: {filename}")

            if total_frames == 0:
                self.log_error(f"No historical data found for filename: {filename}")
                self.progress_bar.setVisible(False)
                return

            if self.selected_channel:
                self.data[self.selected_channel] = {'frequencies': [], 'amplitudes': [], 'phases': []}
            else:
                for ch_name in self.channel_names:
                    self.data[ch_name] = {'frequencies': [], 'amplitudes': [], 'phases': []}

            max_frames = 1500
            batch_size = 50
            sampling_interval = max(1, total_frames // max_frames)
            processed_count = 0
            cursor = history_collection.find(query).sort("frameIndex", 1)

            for history_data in cursor:
                if processed_count % sampling_interval != 0:
                    processed_count += 1
                    continue
                if not self.is_valid_history_data(history_data):
                    processed_count += 1
                    continue

                main_channels = history_data.get("numberOfChannels", 0)
                samples_per_channel = history_data.get("samplingSize", 0)
                tacho_channels = history_data.get("tacoChannelCount", 0)
                freq_start_idx = main_channels * samples_per_channel
                trigger_start_idx = freq_start_idx + samples_per_channel

                if self.selected_channel:
                    ch_idx = self.channel_indices.get(self.selected_channel)
                    if ch_idx is not None and ch_idx < main_channels:
                        channel_data = [history_data["message"][i * main_channels + ch_idx] * self.scaling_factor
                                       for i in range(samples_per_channel)]
                        freq_data = [history_data["message"][freq_start_idx + i]
                                     for i in range(samples_per_channel) if freq_start_idx + i < len(history_data["message"])]
                        trigger_data = [history_data["message"][trigger_start_idx + i]
                                        for i in range(samples_per_channel) if trigger_start_idx + i < len(history_data["message"])]
                        self.log_info(f"Processing historical data for {self.selected_channel}: {len(channel_data)} samples")
                        self.process_data(channel_data, freq_data, trigger_data, self.selected_channel)
                    else:
                        self.log_error(f"Invalid channel index {ch_idx} for {self.selected_channel}")
                else:
                    for ch_idx, ch_name in enumerate(self.channel_names):
                        if ch_idx >= main_channels:
                            continue
                        channel_data = [history_data["message"][i * main_channels + ch_idx] * self.scaling_factor
                                       for i in range(samples_per_channel)]
                        freq_data = [history_data["message"][freq_start_idx + i]
                                     for i in range(samples_per_channel) if freq_start_idx + i < len(history_data["message"])]
                        trigger_data = [history_data["message"][trigger_start_idx + i]
                                        for i in range(samples_per_channel) if trigger_start_idx + i < len(history_data["message"])]
                        self.log_info(f"Processing historical data for {ch_name}: {len(channel_data)} samples")
                        self.process_data(channel_data, freq_data, trigger_data, ch_name)

                processed_count += 1
                self.progress_bar.setValue(int((processed_count / total_frames) * 100))
                if processed_count % batch_size == 0:
                    self.update_plots()

            self.update_plots()
            self.progress_bar.setVisible(False)
            self.log_info(f"Processed {processed_count}/{total_frames} frames for {filename}")
            client.close()
        except Exception as e:
            self.log_error(f"Error processing historical data: {str(e)}")
            self.progress_bar.setVisible(False)

    def is_valid_history_data(self, history_data):
        try:
            main_channels = history_data.get("numberOfChannels", 0)
            samples_per_channel = history_data.get("samplingSize", 0)
            tacho_channels = history_data.get("tacoChannelCount", 0)
            message = history_data.get("message", [])
            valid = (main_channels > 0 and
                     samples_per_channel > 0 and
                     len(message) >= (main_channels + tacho_channels) * samples_per_channel)
            if not valid:
                self.log_error(f"Invalid history data: channels={main_channels}, samples={samples_per_channel}, message_len={len(message)}")
            return valid
        except Exception as e:
            self.log_error(f"Error validating history data: {str(e)}")
            return False

    def get_widget(self):
        return self.widget

    def cleanup(self):
        self.update_timer.stop()
        for ch_name in self.channel_names:
            self.data[ch_name].clear()
        self.plots.clear()
        self.plot_widgets.clear()
        self.log_info("Cleaned up BodePlotFeature")