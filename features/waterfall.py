from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import numpy as np
import math
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class WaterfallFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None, channel_count=None):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.model_name = model_name
        self.console = console
        self.widget = None
        try:
            self.channel_count = int(channel_count) if channel_count is not None else self.get_channel_count_from_db()
            if self.channel_count <= 0:
                raise ValueError(f"Invalid channel count: {self.channel_count}")
        except (ValueError, TypeError) as e:
            self.channel_count = self.get_channel_count_from_db()
            if self.console:
                self.console.append_to_console(f"Invalid channel_count {channel_count}: {str(e)}. Using {self.channel_count} from database.")
            logging.error(f"Invalid channel_count {channel_count}: {str(e)}. Using {self.channel_count} from database.")
        # Tacho channels: try to read from DB; fallback to 2
        self.tacho_channels_count = self.get_tacho_count_from_db(default=2)
        self.main_channels = max(0, self.channel_count - self.tacho_channels_count)
        self.max_lines = 1
        self.data_history = [[] for _ in range(self.main_channels if self.main_channels > 0 else self.channel_count)]
        self.phase_history = [[] for _ in range(self.main_channels if self.main_channels > 0 else self.channel_count)]
        self.scaling_factor = 3.3 / 65535.0
        self.sample_rate = 4096
        self.samples_per_channel = 4096
        self.last_frame_index = -1
        self.frequency_range = (0, 2000)
        # Load channel names from DB for the opened project/model
        self.channel_names = self.get_channel_names()
        self.initUI()
        if self.console:
            self.console.append_to_console(
                f"Initialized WaterfallFeature for {self.model_name or 'No Model'} with {self.channel_count} channels (main={self.main_channels}): {self.channel_names}"
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

    def get_channel_names(self):
        try:
            project_data = self.db.get_project_data(self.project_name) if self.db else {}
            model = next((m for m in project_data.get("models", []) if m["name"] == self.model_name), None)
            if model:
                return [c.get("channelName", f"Channel_{i+1}") for i, c in enumerate(model.get("channels", []))]
            return [f"Channel_{i+1}" for i in range(self.channel_count)]
        except Exception as e:
            if self.console:
                self.console.append_to_console(f"Error retrieving channel names: {str(e)}")
            logging.error(f"Error retrieving channel names: {str(e)}")
            return [f"Channel_{i+1}" for i in range(self.channel_count)]

    def get_tacho_count_from_db(self, default=2):
        try:
            project_data = self.db.get_project_data(self.project_name) if self.db else {}
            model = next((m for m in project_data.get("models", []) if m["name"] == self.model_name), None)
            if model:
                # Common field name used elsewhere: 'tacoChannelCount'
                val = model.get("tacoChannelCount")
                if isinstance(val, int) and val >= 0:
                    return val
                # Alternate spellings just in case
                for key in ["tachoChannelCount", "tachChannelCount", "tachometerChannels"]:
                    v = model.get(key)
                    if isinstance(v, int) and v >= 0:
                        return v
        except Exception:
            pass
        return default

    def initUI(self):
        self.widget = QWidget()
        layout = QVBoxLayout()
        self.widget.setLayout(layout)
        # Enable constrained layout to better accommodate axis decorations
        self.figure = Figure(figsize=(8, 6), constrained_layout=True)
        self.canvas = FigureCanvas(self.figure)
        self.ax = self.figure.add_subplot(111, projection='3d')
        layout.addWidget(self.canvas)
        self.toolbar = NavigationToolbar(self.canvas, self.widget)
        layout.addWidget(self.toolbar)
        if not self.model_name and self.console:
            self.console.append_to_console("No model selected in WaterfallFeature.")

    def get_widget(self):
        return self.widget

    def on_data_received(self, tag_name, model_name, values, sample_rate, frame_index):
        if self.model_name != model_name:
            if self.console:
                self.console.append_to_console(f"WaterfallFeature: Ignored data for model {model_name}, expected {self.model_name}, frame {frame_index}")
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
                    self.console.append_to_console(f"WaterfallFeature: Empty values for frame {frame_index}")
                return

            if isinstance(values[0], (list, np.ndarray)):
                # Full channels mode
                total_channels = len(values)
                if total_channels < self.channel_count:
                    if self.console:
                        self.console.append_to_console(
                            f"WaterfallFeature: Received {total_channels} channels, expected at least {self.channel_count}, frame {frame_index}"
                        )
                    return
                # Update channel count dynamically if needed
                if total_channels != self.channel_count:
                    if self.console:
                        self.console.append_to_console(
                            f"WaterfallFeature: Adjusting channel count from {self.channel_count} to {total_channels} based on payload, frame {frame_index}"
                        )
                    self.channel_count = total_channels
                    self.main_channels = max(0, self.channel_count - self.tacho_channels_count)
                    # Refresh names from DB to reflect the current project/model
                    try:
                        self.channel_names = self.get_channel_names()
                    except Exception:
                        # Fallback safe labels if DB not available temporarily
                        self.channel_names = [f"Channel_{i+1}" for i in range(self.channel_count)]
                    self.data_history = [[] for _ in range(self.main_channels)]
                    self.phase_history = [[] for _ in range(self.main_channels)]
                # Use only main channels (exclude last tacho channels)
                channel_data = values[:self.main_channels] if self.main_channels > 0 else values
            else:
                # Per channel mode - not expected for waterfall (requires all channels)
                if self.console:
                    self.console.append_to_console(
                        f"WaterfallFeature: Received per-channel data, expected full channels, skipping frame {frame_index}"
                    )
                return

            self.sample_rate = sample_rate if sample_rate > 0 else 4096
            self.samples_per_channel = len(channel_data[0]) if channel_data and channel_data[0] else 4096
            sample_count = self.samples_per_channel
            target_length = 2 ** math.ceil(math.log2(sample_count))
            fft_magnitudes = []
            fft_phases = []
            frequencies = np.fft.fftfreq(target_length, 1.0 / self.sample_rate)[:target_length // 2]
            freq_mask = (frequencies >= self.frequency_range[0]) & (frequencies <= self.frequency_range[1])
            filtered_frequencies = frequencies[freq_mask]

            if len(filtered_frequencies) == 0:
                if self.console:
                    self.console.append_to_console(f"Error: No valid frequencies in range {self.frequency_range}, frame {frame_index}")
                return
            # Iterate only over main channels
            active_channels = self.main_channels if self.main_channels > 0 else len(channel_data)
            for ch_idx in range(active_channels):
                if len(channel_data[ch_idx]) != self.samples_per_channel:
                    if self.console:
                        self.console.append_to_console(
                            f"Invalid data length for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}: got {len(channel_data[ch_idx])}, expected {self.samples_per_channel}, frame {frame_index}"
                        )
                    continue
                data = np.array(channel_data[ch_idx], dtype=np.float32) * self.scaling_factor
                if not np.any(data):
                    if self.console:
                        self.console.append_to_console(
                            f"Warning: Zero data for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}, frame {frame_index}"
                        )
                    continue

                padded_data = np.pad(data, (0, target_length - sample_count), mode='constant') if target_length > sample_count else data
                fft_result = np.fft.fft(padded_data)
                half = target_length // 2
                magnitudes = (2.0 / target_length) * np.abs(fft_result[:half])
                magnitudes[0] /= 2
                if target_length % 2 == 0:
                    magnitudes[-1] /= 2
                phases = np.angle(fft_result[:half], deg=True)
                filtered_magnitudes = magnitudes[freq_mask]
                filtered_phases = phases[freq_mask]
                if len(filtered_frequencies) > 1600:
                    indices = np.linspace(0, len(filtered_frequencies) - 1, 1600, dtype=int)
                    filtered_frequencies_subset = filtered_frequencies[indices]
                    filtered_magnitudes = filtered_magnitudes[indices]
                    filtered_phases = filtered_phases[indices]
                else:
                    filtered_frequencies_subset = filtered_frequencies
                if len(filtered_magnitudes) == 0 or len(filtered_frequencies_subset) == 0:
                    if self.console:
                        self.console.append_to_console(
                            f"Error: Empty FFT data for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}, frame {frame_index}"
                        )
                    continue
                # Ensure history buffers sized to active channels
                while len(self.data_history) < active_channels:
                    self.data_history.append([])
                    self.phase_history.append([])
                self.data_history[ch_idx].append(filtered_magnitudes)
                self.phase_history[ch_idx].append(filtered_phases)
                if len(self.data_history[ch_idx]) > self.max_lines:
                    self.data_history[ch_idx].pop(0)
                    self.phase_history[ch_idx].pop(0)
                fft_magnitudes.append(filtered_magnitudes)
                fft_phases.append(filtered_phases)
                if self.console:
                    self.console.append_to_console(
                        f"WaterfallFeature: Processed FFT for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}, "
                        f"samples={len(data)}, Fs={self.sample_rate}Hz, FFT points={len(filtered_magnitudes)}, frame {frame_index}"
                    )
            if fft_magnitudes:
                self.update_waterfall_plot(filtered_frequencies_subset if fft_magnitudes else None)
            else:
                if self.console:
                    self.console.append_to_console(f"No valid FFT data to plot, frame {frame_index}")

        except Exception as e:
            if self.console:
                self.console.append_to_console(f"WaterfallFeature: Error processing data, frame {frame_index}: {str(e)}")
            logging.error(f"WaterfallFeature: Error processing data, frame {frame_index}: {str(e)}")

    def update_waterfall_plot(self, frequencies):
        try:
            self.ax.clear()
            display_channels = self.main_channels if self.main_channels > 0 else self.channel_count
            self.ax.set_title(f"Waterfall Plot")
            self.ax.set_xlabel("Frequency (Hz)")
            self.ax.set_ylabel("Channel")
            self.ax.set_zlabel("Amplitude (V)")
            self.ax.grid(True)
            colors = ['blue', 'red', 'green', 'purple', 'orange', 'cyan', 'magenta', 'yellow', 'black', 'brown']
            max_amplitude = 0
            plotted = False
            active_channels = self.main_channels if self.main_channels > 0 else len(self.data_history)
            ytick_positions = []
            ytick_labels = []
            # Determine labels for main channels from DB names
            labels_source = self.channel_names[:active_channels] if self.channel_names else [f"Channel_{i+1}" for i in range(active_channels)]
            for ch_idx in range(active_channels):
                if not self.data_history[ch_idx]:
                    if self.console:
                        self.console.append_to_console(f"No data to plot for channel {labels_source[ch_idx]}")
                    continue
                num_lines = len(self.data_history[ch_idx])
                for idx, fft_line in enumerate(self.data_history[ch_idx]):
                    if len(fft_line) == 0:
                        if self.console:
                            self.console.append_to_console(f"Empty FFT data for channel {labels_source[ch_idx]}, line {idx}")
                        continue
                    x = frequencies if frequencies is not None and len(frequencies) == len(fft_line) else np.arange(len(fft_line))
                    base_y = ch_idx * (self.max_lines + 2)
                    y = np.full_like(x, base_y)
                    z = fft_line
                    self.ax.plot(x, y, z, color=colors[ch_idx % len(colors)])
                    max_amplitude = max(max_amplitude, np.max(z) if len(z) > 0 else 0)
                    plotted = True
                # Collect one tick per channel at its baseline
                ytick_positions.append(base_y)
                label = labels_source[ch_idx]
                ytick_labels.append(label)
            # Apply custom Y ticks with channel names (remove numbers)
            try:
                self.ax.set_yticks(ytick_positions)
                self.ax.set_yticklabels(ytick_labels)
            except Exception:
                pass
            if not plotted:
                if self.console:
                    self.console.append_to_console("No valid data plotted, drawing empty plot")
                x = np.array([0, 1])
                y = np.array([0, 0])
                z = np.array([0, 0])
                self.ax.plot(x, y, z, color='gray', label='No Data')
            self.ax.set_ylim(-1, active_channels * (self.max_lines + 2))
            self.ax.set_xlim(self.frequency_range[0], self.frequency_range[1] if frequencies is not None else 1000)
            self.ax.set_zlim(0, max_amplitude * 1.1 if max_amplitude > 0 else 1.0)
            # self.ax.legend(loc='upper right')
            self.ax.view_init(elev=20, azim=-45)
            # With constrained_layout=True, tight_layout is not needed

            self.canvas.draw_idle()
            self.canvas.flush_events()
            if self.console:
                self.console.append_to_console(f"WaterfallFeature: Updated plot for {self.channel_count} channels, plotted={plotted}")

        except Exception as e:
            if self.console:
                self.console.append_to_console(f"WaterfallFeature: Error updating plot: {str(e)}")
            logging.error(f"WaterfallFeature: Error updating plot: {str(e)}")

    def load_selected_frame(self, payload: dict):
        try:
            if not payload:
                if self.console:
                    self.console.append_to_console("Waterfall: Invalid selection payload (empty).")
                return
            num_main = int(payload.get("numberOfChannels", 0))
            num_tacho = int(payload.get("tacoChannelCount", 0))
            total_ch = num_main + num_tacho

            Fs = float(payload.get("samplingRate", 0) or 0)
            N = int(payload.get("samplingSize", 0) or 0)
            data_flat = payload.get("message", [])
            if not Fs or not N or not total_ch or not data_flat:
                if self.console:
                    self.console.append_to_console("Waterfall: Incomplete selection payload (Fs/N/channels/data missing).")
                return

            # Shape data into channels if flattened
            if isinstance(data_flat, list) and data_flat and isinstance(data_flat[0], (int, float)):
                if len(data_flat) != total_ch * N:
                    if self.console:
                        self.console.append_to_console(f"Waterfall: Data length mismatch. expected {total_ch*N}, got {len(data_flat)}")
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
                        self.console.append_to_console("Waterfall: Invalid nested data shape in selection payload.")
                    return

            # Update channel count dynamically if mismatched
            if num_main + num_tacho != self.channel_count:
                if self.console:
                    self.console.append_to_console(f"Waterfall: Adjusting channel count from {self.channel_count} to {total_ch} based on payload.")
                self.channel_count = total_ch
            # Always compute main channels from payload fields
            self.tacho_channels_count = num_tacho
            self.main_channels = max(0, num_main)
            # Refresh DB channel names for current model
            try:
                self.channel_names = self.get_channel_names()
            except Exception:
                self.channel_names = [f"Channel_{i+1}" for i in range(self.channel_count)]
            # Ensure buffers sized to main channels
            self.data_history = [[] for _ in range(self.main_channels if self.main_channels > 0 else num_main)]
            self.phase_history = [[] for _ in range(self.main_channels if self.main_channels > 0 else num_main)]

            self.sample_rate = Fs
            self.samples_per_channel = N
            sample_count = self.samples_per_channel
            target_length = 2 ** math.ceil(math.log2(sample_count))
            fft_magnitudes = []
            fft_phases = []
            frequencies = np.fft.fftfreq(target_length, 1.0 / self.sample_rate)[:target_length // 2]
            freq_mask = (frequencies >= self.frequency_range[0]) & (frequencies <= self.frequency_range[1])
            filtered_frequencies = frequencies[freq_mask]
            if len(filtered_frequencies) == 0:
                if self.console:
                    self.console.append_to_console(f"Waterfall: Error: No valid frequencies in range {self.frequency_range}")
                return
            for ch_idx in range(self.main_channels):
                data = np.array(values[ch_idx], dtype=np.float32) * self.scaling_factor
                if not np.any(data):
                    if self.console:
                        self.console.append_to_console(f"Waterfall: Warning: Zero data for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}")
                    continue
                padded_data = np.pad(data, (0, target_length - sample_count), mode='constant') if target_length > sample_count else data
                fft_result = np.fft.fft(padded_data)
                half = target_length // 2
                magnitudes = (2.0 / target_length) * np.abs(fft_result[:half])

                magnitudes[0] /= 2
                if target_length % 2 == 0:
                    magnitudes[-1] /= 2
                phases = np.angle(fft_result[:half], deg=True)
                filtered_magnitudes = magnitudes[freq_mask]
                filtered_phases = phases[freq_mask]
                if len(filtered_frequencies) > 1600:
                    indices = np.linspace(0, len(filtered_frequencies) - 1, 1600, dtype=int)
                    filtered_frequencies_subset = filtered_frequencies[indices]
                    filtered_magnitudes = filtered_magnitudes[indices]
                    filtered_phases = filtered_phases[indices]
                else:
                    filtered_frequencies_subset = filtered_frequencies
                if len(filtered_magnitudes) == 0 or len(filtered_frequencies_subset) == 0:
                    if self.console:
                        self.console.append_to_console(f"Waterfall: Error: Empty FFT data for channel {self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else ch_idx}")
                    continue
                self.data_history[ch_idx] = [filtered_magnitudes]
                self.phase_history[ch_idx] = [filtered_phases]
                fft_magnitudes.append(filtered_magnitudes)
                fft_phases.append(filtered_phases)
                if self.console:
                    label = self.channel_names[ch_idx] if ch_idx < len(self.channel_names) else f"Channel_{ch_idx+1}"
                    self.console.append_to_console(
                        f"Waterfall: Processed FFT for channel {label}, samples={len(data)}, Fs={self.sample_rate}Hz, FFT points={len(filtered_magnitudes)}"
                    )
            if fft_magnitudes:
                self.update_waterfall_plot(filtered_frequencies_subset if fft_magnitudes else None)
                if self.console:
                    self.console.append_to_console(f"Waterfall: Loaded selected frame {payload.get('frameIndex')} ({N} samples @ {Fs}Hz) for {self.main_channels} main channels")
            else:
                if self.console:
                    self.console.append_to_console("Waterfall: No valid FFT data to plot from selected frame")

        except Exception as e:
            if self.console:
                self.console.append_to_console(f"Waterfall: Error loading selected frame: {str(e)}")
            logging.error(f"Waterfall: Error loading selected frame: {str(e)}")

    def cleanup(self):
        try:
            self.canvas.figure.clear()
            self.canvas.deleteLater()
            self.toolbar.deleteLater()
            self.widget.deleteLater()
            self.data_history = [[] for _ in range(self.channel_count)]
            self.phase_history = [[] for _ in range(self.channel_count)]
            if self.console:
                self.console.append_to_console(f"WaterfallFeature: Cleaned up resources")
        except Exception as e:
            if self.console:
                self.console.append_to_console(f"Error cleaning up WaterfallFeature: {str(e)}")
            logging.error(f"Error cleaning up WaterfallFeature: {str(e)}")

    def refresh_channel_properties(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.project_name)
            model = next((m for m in project_data.get("models", []) if m["name"] == self.model_name), None)
            if model:
                new_channel_names = [ch.get("channelName", f"Channel_{i+1}") for i, ch in enumerate(model.get("channels", []))]
                new_channel_count = len(new_channel_names)
                if new_channel_count != self.channel_count:
                    if self.console:
                        self.console.append_to_console(
                            f"Channel count updated from {self.channel_count} to {new_channel_count} for model {self.model_name}"
                        )
                    self.channel_count = new_channel_count
                    self.channel_names = new_channel_names
                    self.data_history = [[] for _ in range(self.channel_count)]
                    self.phase_history = [[] for _ in range(self.channel_count)]
                else:
                    self.channel_names = new_channel_names
                if self.console:
                    self.console.append_to_console(f"Refreshed channel properties: {self.channel_count} channels: {self.channel_names}")
        except Exception as e:
            if self.console:
                self.console.append_to_console(f"Error refreshing channel properties: {str(e)}")
            logging.error(f"Error refreshing channel properties: {str(e)}") 