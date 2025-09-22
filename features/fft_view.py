## Updated fft_view.py

from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QLineEdit, QGridLayout, QComboBox
from PyQt5.QtGui import QDoubleValidator, QIntValidator
from PyQt5.QtCore import QTimer, Qt
import pyqtgraph as pg
import numpy as np
import logging
from utils.signal_calibration import counts_to_volts, calibrate, convert_unit
from scipy.fft import fft
from scipy.signal import get_window
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class FFTSettings:
    def __init__(self, project_id):
        self.project_id = project_id
        self.window_type = "Hamming"
        self.start_frequency = 10.0
        self.stop_frequency = 2000.0
        self.number_of_lines = 1600
        self.overlap_percentage = 0.0
        self.averaging_mode = "No Averaging"
        self.number_of_averages = 10
        self.weighting_mode = "Linear"
        self.linear_mode = "Continuous"
        self.updated_at = datetime.utcnow()

class FFTViewFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None, layout="vertical", channel_count=4):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.channel_name = channel
        self.model_name = model_name
        self.console = console

        self.widget = None
        self.magnitude_plot_widget = None
        self.phase_plot_widget = None
        self.magnitude_plot_item = None
        self.phase_plot_item = None

        self.sample_rate = 1000
        self.channel_index = self.resolve_channel_index(channel) if channel is not None else None
        self.latest_data = None

        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_plot)
        self.update_interval = 200
        self.max_samples = 4096
        self.layout_type = layout

        self.mongo_client = self.db.client
        self.project_id = None
        self.settings = FFTSettings(None)
        self.data_buffer = []

        # Calibration and channel metadata
        self.scaling_factor = 3.3 / 65535.0
        self.off_set = 32768.0
        self.channel_properties = {}
        self.channel_names = []
        self._y_unit_label = None

        self.settings_panel = None
        self.settings_button = None
        self.channel_count = channel_count
        self.last_frame_index = -1
        self.is_saving = False
        self.current_filename = None

        self.initUI()
        self.initialize_async()

        if self.console:
            self.console.append_to_console(f"Initialized FFTViewFeature for {self.model_name}/{self.channel_name or 'No Channel'} with {self.channel_count} channels")

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
        main_layout = QVBoxLayout()
        self.widget.setLayout(main_layout)

        top_layout = QHBoxLayout()
        top_layout.addStretch()
        self.settings_button = QPushButton("⚙️ Settings")
        self.settings_button.setStyleSheet("""
        QPushButton {
            background-color: #4CAF50;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 14px;
            min-width: 120px;
        }
        QPushButton:hover { background-color: #45a049; }
        QPushButton:pressed { background-color: #3d8b40; }
        """)
        self.settings_button.clicked.connect(self.toggle_settings)
        top_layout.addWidget(self.settings_button)
        main_layout.addLayout(top_layout)

        self.settings_panel = QWidget()
        self.settings_panel.setStyleSheet("""
        QWidget {
            background-color: #f5f5f5;
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            padding: 10px;
        }
        """)
        self.settings_panel.setVisible(False)

        settings_layout = QGridLayout()
        settings_layout.setSpacing(10)
        self.settings_panel.setLayout(settings_layout)

        self.settings_widgets = {}

        window_label = QLabel("Window Type")
        window_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(window_label, 0, 0)
        window_combo = QComboBox()
        window_combo.addItems(["Hamming", "Hanning", "Blackman", "Flat-top", "None"])
        window_combo.setCurrentText(self.settings.window_type)
        window_combo.setStyleSheet("""
        QComboBox { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(window_combo, 0, 1)
        self.settings_widgets["WindowType"] = window_combo

        start_freq_label = QLabel("Start Frequency (Hz)")
        start_freq_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(start_freq_label, 1, 0)
        start_freq_edit = QLineEdit(str(self.settings.start_frequency))
        start_freq_edit.setValidator(QDoubleValidator(0.0, 10000.0, 2))
        start_freq_edit.setStyleSheet("""
        QLineEdit { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(start_freq_edit, 1, 1)
        self.settings_widgets["StartFrequency"] = start_freq_edit

        stop_freq_label = QLabel("Stop Frequency (Hz)")
        stop_freq_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(stop_freq_label, 2, 0)
        stop_freq_edit = QLineEdit(str(self.settings.stop_frequency))
        stop_freq_edit.setValidator(QDoubleValidator(0.0, 10000.0, 2))
        stop_freq_edit.setStyleSheet("""
        QLineEdit { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(stop_freq_edit, 2, 1)
        self.settings_widgets["StopFrequency"] = stop_freq_edit

        lines_label = QLabel("Number of Lines")
        lines_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(lines_label, 3, 0)
        lines_edit = QLineEdit(str(self.settings.number_of_lines))
        lines_edit.setValidator(QIntValidator(100, 3200))
        lines_edit.setStyleSheet("""
        QLineEdit { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(lines_edit, 3, 1)
        self.settings_widgets["NumberOfLines"] = lines_edit

        overlap_label = QLabel("Overlap Percentage (%)")
        overlap_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(overlap_label, 4, 0)
        overlap_edit = QLineEdit(str(self.settings.overlap_percentage))
        overlap_edit.setValidator(QDoubleValidator(0.0, 99.9, 2))
        overlap_edit.setStyleSheet("""
        QLineEdit { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(overlap_edit, 4, 1)
        self.settings_widgets["OverlapPercentage"] = overlap_edit

        avg_mode_label = QLabel("Averaging Mode")
        avg_mode_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(avg_mode_label, 5, 0)
        avg_mode_combo = QComboBox()
        avg_mode_combo.addItems(["No Averaging", "Linear", "Exponential"])
        avg_mode_combo.setCurrentText(self.settings.averaging_mode)
        avg_mode_combo.setStyleSheet("""
        QComboBox { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(avg_mode_combo, 5, 1)
        self.settings_widgets["AveragingMode"] = avg_mode_combo

        avg_num_label = QLabel("Number of Averages")
        avg_num_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(avg_num_label, 6, 0)
        avg_num_edit = QLineEdit(str(self.settings.number_of_averages))
        avg_num_edit.setValidator(QIntValidator(1, 100))
        avg_num_edit.setStyleSheet("""
        QLineEdit { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(avg_num_edit, 6, 1)
        self.settings_widgets["NumberOfAverages"] = avg_num_edit

        weight_label = QLabel("Weighting Mode")
        weight_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(weight_label, 7, 0)
        weight_combo = QComboBox()
        weight_combo.addItems(["Linear", "A-Weighting", "B-Weighting", "C-Weighting"])
        weight_combo.setCurrentText(self.settings.weighting_mode)
        weight_combo.setStyleSheet("""
        QComboBox { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(weight_combo, 7, 1)
        self.settings_widgets["WeightingMode"] = weight_combo

        linear_label = QLabel("Linear Mode")
        linear_label.setStyleSheet("font-size: 14px;")
        settings_layout.addWidget(linear_label, 8, 0)
        linear_combo = QComboBox()
        linear_combo.addItems(["Continuous", "Peak Hold", "Time Synchronous"])
        linear_combo.setCurrentText(self.settings.linear_mode)
        linear_combo.setStyleSheet("""
        QComboBox { padding: 5px; border: 1px solid #d0d0d0; border-radius: 4px; background-color: white; min-width: 100px; }
        """)
        settings_layout.addWidget(linear_combo, 8, 1)
        self.settings_widgets["LinearMode"] = linear_combo

        save_button = QPushButton("Save")
        save_button.setStyleSheet("""
        QPushButton { background-color: #2196F3; color: white; border: none; padding: 8px 16px; border-radius: 4px; font-size: 14px; min-width: 100px; }
        QPushButton:hover { background-color: #1e88e5; }
        QPushButton:pressed { background-color: #1976d2; }
        """)
        save_button.clicked.connect(self.save_settings)

        close_button = QPushButton("Close")
        close_button.setStyleSheet("""
        QPushButton { background-color: #f44336; color: white; border: none; padding: 8px 16px; border-radius: 4px; font-size: 14px; min-width: 100px; }
        QPushButton:hover { background-color: #e53935; }
        QPushButton:pressed { background-color: #d32f2f; }
        """)
        close_button.clicked.connect(self.close_settings)

        settings_layout.addWidget(save_button, 9, 0)
        settings_layout.addWidget(close_button, 9, 1)
        main_layout.addWidget(self.settings_panel)

        plot_layout = QHBoxLayout() if self.layout_type == "horizontal" else QVBoxLayout()

        pg.setConfigOptions(antialias=False)

        self.magnitude_plot_widget = pg.PlotWidget()
        self.magnitude_plot_widget.setBackground("white")
        display_channel = self.channel_name if self.channel_name else f"Channel_{self.channel_index + 1}" if self.channel_index is not None else "Unknown"
        self.magnitude_plot_widget.setTitle(f"Magnitude Spectrum - {self.model_name or 'Unknown'} - {display_channel}", color="black", size="12pt")
        self.magnitude_plot_widget.setLabel('left', 'Amplitude', color='#000000')
        self.magnitude_plot_widget.setLabel('bottom', 'Frequency (Hz)', color='#000000')
        self.magnitude_plot_widget.showGrid(x=True, y=True)
        self.magnitude_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)
        self.magnitude_plot_widget.enableAutoRange('y', True)
        self.magnitude_plot_item = self.magnitude_plot_widget.plot(pen=pg.mkPen(color='#4a90e2', width=2))
        plot_layout.addWidget(self.magnitude_plot_widget)

        self.phase_plot_widget = pg.PlotWidget()
        self.phase_plot_widget.setBackground("white")
        self.phase_plot_widget.setTitle(f"Phase Spectrum - {self.model_name or 'Unknown'} - {display_channel}", color="black", size="12pt")
        self.phase_plot_widget.setLabel('left', 'Phase (degrees)', color='#000000')
        self.phase_plot_widget.setLabel('bottom', 'Frequency (Hz)', color='#000000')
        self.phase_plot_widget.showGrid(x=True, y=True)
        self.phase_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)
        self.phase_plot_widget.enableAutoRange('y', True)
        self.phase_plot_item = self.phase_plot_widget.plot(pen=pg.mkPen(color='#e74c3c', width=2))
        plot_layout.addWidget(self.phase_plot_widget)

        main_layout.addLayout(plot_layout)

        self.update_timer.start(self.update_interval)

    def initialize_async(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.project_name)
            self.project_id = project_data.get("_id")
            self.settings = self.load_settings_from_database() or FFTSettings(self.project_id)
            # Load channel properties so we can calibrate magnitudes per unit
            self.load_channel_properties()
            self.update_settings_ui()
        except Exception as e:
            logging.error(f"Error initializing FFT settings: {str(e)}")
            if self.console:
                self.console.append_to_console(f"Error initializing FFT settings: {str(e)}")

    def load_channel_properties(self):
        try:
            project_data = self.db.get_project_data(self.project_name) if self.db else None
            if not project_data:
                return
            for model in project_data.get("models", []):
                if model.get("name") == self.model_name:
                    self.channel_names = [ch.get("channelName") for ch in model.get("channels", [])]
                    for ch in model.get("channels", []):
                        name = ch.get("channelName")
                        unit = (ch.get("unit", "mil") or "mil").lower()
                        correction_value = float(ch.get("correctionValue", "1.0") or "1.0")
                        gain = float(ch.get("gain", "1.0") or "1.0")
                        sensitivity = float(ch.get("sensitivity", "1.0") or "1.0")
                        self.channel_properties[name] = {
                            "unit": unit,
                            "correctionValue": correction_value,
                            "gain": gain,
                            "sensitivity": sensitivity,
                        }
                    break
        except Exception as e:
            logging.error(f"FFT: Error loading channel properties: {e}")

    def load_settings_from_database(self):
        try:
            collection = self.mongo_client["changed_db"]["fft_settings"]
            settings_data = collection.find_one({"project_id": self.project_id})
            if settings_data:
                self.settings = FFTSettings(self.project_id)
                self.settings.window_type = settings_data.get("window_type", "Hamming")
                self.settings.start_frequency = settings_data.get("start_frequency", 10.0)
                self.settings.stop_frequency = settings_data.get("stop_frequency", 2000.0)
                self.settings.number_of_lines = settings_data.get("number_of_lines", 1600)
                self.settings.overlap_percentage = settings_data.get("overlap_percentage", 0.0)
                self.settings.averaging_mode = settings_data.get("averaging_mode", "No Averaging")
                self.settings.number_of_averages = settings_data.get("number_of_averages", 10)
                self.settings.weighting_mode = settings_data.get("weighting_mode", "Linear")
                self.settings.linear_mode = settings_data.get("linear_mode", "Continuous")
                self.settings.updated_at = settings_data.get("updated_at", datetime.utcnow())
                return self.settings
            return None
        except Exception as e:
            logging.error(f"Error loading FFT settings from database: {str(e)}")
            return None

    def save_settings_to_database(self):
        try:
            collection = self.mongo_client["sarayu"]["fft_settings"]
            self.settings.updated_at = datetime.utcnow()
            settings_dict = vars(self.settings)
            collection.update_one(
                {"project_id": self.project_id},
                {"$set": settings_dict},
                upsert=True
            )
        except Exception as e:
            logging.error(f"Error saving FFT settings to database: {str(e)}")

    def update_settings_ui(self):
        if self.settings_widgets:
            self.settings_widgets["WindowType"].setCurrentText(self.settings.window_type)
            self.settings_widgets["StartFrequency"].setText(str(self.settings.start_frequency))
            self.settings_widgets["StopFrequency"].setText(str(self.settings.stop_frequency))
            self.settings_widgets["NumberOfLines"].setText(str(self.settings.number_of_lines))
            self.settings_widgets["OverlapPercentage"].setText(str(self.settings.overlap_percentage))
            self.settings_widgets["AveragingMode"].setCurrentText(self.settings.averaging_mode)
            self.settings_widgets["NumberOfAverages"].setText(str(self.settings.number_of_averages))
            self.settings_widgets["WeightingMode"].setCurrentText(self.settings.weighting_mode)
            self.settings_widgets["LinearMode"].setCurrentText(self.settings.linear_mode)

    def toggle_settings(self):
        self.settings_panel.setVisible(not self.settings_panel.isVisible())
        self.settings_button.setVisible(not self.settings_panel.isVisible())

    def save_settings(self):
        try:
            self.settings.window_type = self.settings_widgets["WindowType"].currentText()
            self.settings.start_frequency = float(self.settings_widgets["StartFrequency"].text() or 10.0)
            self.settings.stop_frequency = float(self.settings_widgets["StopFrequency"].text() or 2000.0)
            self.settings.number_of_lines = int(self.settings_widgets["NumberOfLines"].text() or 1600)
            self.settings.overlap_percentage = float(self.settings_widgets["OverlapPercentage"].text() or 0.0)
            self.settings.averaging_mode = self.settings_widgets["AveragingMode"].currentText()
            self.settings.number_of_averages = int(self.settings_widgets["NumberOfAverages"].text() or 10)
            self.settings.weighting_mode = self.settings_widgets["WeightingMode"].currentText()
            self.settings.linear_mode = self.settings_widgets["LinearMode"].currentText()

            if self.settings.start_frequency >= self.settings.stop_frequency:
                self.settings.start_frequency = 10.0
                self.settings.stop_frequency = 2000.0
                self.settings_widgets["StartFrequency"].setText(str(self.settings.start_frequency))
                self.settings_widgets["StopFrequency"].setText(str(self.settings.stop_frequency))
                self.log_and_set_status("Invalid frequency range, reset to defaults.")

            if self.settings.number_of_lines < 100 or self.settings.number_of_lines > 3200:
                self.settings.number_of_lines = 1600
                self.settings_widgets["NumberOfLines"].setText(str(self.settings.number_of_lines))
                self.log_and_set_status("Invalid number of lines, reset to default.")

            if self.settings.overlap_percentage < 0 or self.settings.overlap_percentage > 99.9:
                self.settings.overlap_percentage = 0.0
                self.settings_widgets["OverlapPercentage"].setText(str(self.settings.overlap_percentage))
                self.log_and_set_status("Invalid overlap percentage, reset to default.")

            if self.settings.number_of_averages < 1 or self.settings.number_of_averages > 100:
                self.settings.number_of_averages = 10
                self.settings_widgets["NumberOfAverages"].setText(str(self.settings.number_of_averages))
                self.log_and_set_status("Invalid number of averages, reset to default.")

            self.save_settings_to_database()
            self.magnitude_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)
            self.phase_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)
            self.settings_panel.setVisible(False)
            self.settings_button.setVisible(True)
            if self.console:
                self.console.append_to_console("FFT settings updated and saved.")
            self.update_plot()
        except Exception as e:
            self.log_and_set_status(f"Error saving FFT settings: {str(e)}")

    def close_settings(self):
        self.settings_widgets["WindowType"].setCurrentText(self.settings.window_type)
        self.settings_widgets["StartFrequency"].setText(str(self.settings.start_frequency))
        self.settings_widgets["StopFrequency"].setText(str(self.settings.stop_frequency))
        self.settings_widgets["NumberOfLines"].setText(str(self.settings.number_of_lines))
        self.settings_widgets["OverlapPercentage"].setText(str(self.settings.overlap_percentage))
        self.settings_widgets["AveragingMode"].setCurrentText(self.settings.averaging_mode)
        self.settings_widgets["NumberOfAverages"].setText(str(self.settings.number_of_averages))
        self.settings_widgets["WeightingMode"].setCurrentText(self.settings.weighting_mode)
        self.settings_widgets["LinearMode"].setCurrentText(self.settings.linear_mode)
        self.settings_panel.setVisible(False)
        self.settings_button.setVisible(True)

    def get_widget(self):
        return self.widget

    def on_data_received(self, tag_name, model_name, values, sample_rate, frame_index):
        if self.model_name != model_name or self.channel_index is None:
            if self.console:
                self.console.append_to_console(
                    f"FFT View: Skipped data - model_name={model_name} (expected {self.model_name}), "
                    f"channel_index={self.channel_index}, frame {frame_index}"
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
                return
            if isinstance(values[0], (list, np.ndarray)):
                # Full channels mode
                if len(values) < self.channel_count:
                    self.log_and_set_status(f"Received {len(values)} channels, expected at least {self.channel_count}, frame {frame_index}")
                    return
                if self.channel_index >= len(values):
                    self.log_and_set_status(f"Channel index {self.channel_index} out of range for {len(values)} channels, frame {frame_index}")
                    return
                channel_data = values[self.channel_index]
            else:
                # Per channel mode
                channel_data = values

            # Sample rate
            self.sample_rate = sample_rate if sample_rate > 0 else 1000
            # Counts -> volts (center around 0V) using shared helper
            raw_counts = np.array(channel_data[:self.max_samples], dtype=np.float64)
            volts = counts_to_volts(raw_counts, self.scaling_factor, self.off_set)

            # Determine channel props
            ch_name = None
            if self.channel_index is not None and 0 <= self.channel_index < len(self.channel_names):
                ch_name = self.channel_names[self.channel_index]
            props = self.channel_properties.get(ch_name or str(self.channel_index), {
                "unit": "mil", "correctionValue": 1.0, "gain": 1.0, "sensitivity": 1.0
            })
            try:
                base_value = calibrate(volts, props.get("correctionValue", 1.0), props.get("gain", 1.0), props.get("sensitivity", 1.0))
            except Exception:
                base_value = volts
            unit = (props.get("unit", "mil") or "mil").lower()
            calibrated = convert_unit(base_value, unit, "Displacement")

            # Update buffers
            self.latest_data = calibrated.astype(np.float64)
            self.data_buffer.append(self.latest_data.copy())
            if len(self.data_buffer) > max(int(self.settings.number_of_averages), 1):
                self.data_buffer = self.data_buffer[-int(self.settings.number_of_averages):]

            # Update axis label once
            if self._y_unit_label != unit:
                self._y_unit_label = unit
                self.magnitude_plot_widget.setLabel('left', f'Amplitude ({unit})', color='#000000')

            if self.is_saving and self.current_filename:
                self.save_data_to_database(tag_name, values, sample_rate, frame_index)

            if self.console:
                self.console.append_to_console(
                    f"FFT View: Received data for channel {self.channel_name or self.channel_index}, "
                    f"samples={len(self.latest_data)}, Fs={self.sample_rate}Hz, frame {frame_index}"
                )
        except Exception as e:
            self.log_and_set_status(f"Error in on_data_received, frame {frame_index}: {str(e)}")

    def update_plot(self):
        if not self.data_buffer:
            return
        try:
            data = self.data_buffer[-1] if self.settings.averaging_mode == "No Averaging" else np.mean(self.data_buffer, axis=0)
            n = len(data)
            if n < 2:
                self.log_and_set_status(f"Insufficient data length: {n}")
                return

            # Map UI window names to scipy.signal.get_window names
            if self.settings.window_type == "None":
                window = np.ones(n)
                window_name = "rectangular"
            else:
                ui_name = self.settings.window_type.lower()
                if ui_name == "hanning":
                    mapped = "hann"
                elif ui_name == "flat-top":
                    mapped = "flattop"
                else:
                    mapped = ui_name
                window = get_window(mapped, n)
                window_name = mapped
            windowed_data = data * window

            target_length = 2 ** int(np.ceil(np.log2(n)))
            padded_data = np.zeros(target_length)
            padded_data[:n] = windowed_data

            fft_result = fft(padded_data)
            half = target_length // 2
            frequencies = np.linspace(0, self.sample_rate / 2, half)
            freq_mask = (frequencies >= self.settings.start_frequency) & (frequencies <= self.settings.stop_frequency)

            filtered_frequencies = frequencies[freq_mask]
            # Single-sided amplitude spectrum with coherent gain compensation
            # Coherent gain (CG) = mean(window)
            cg = np.mean(window)
            magnitudes = np.abs(fft_result[:half]) / (target_length * max(cg, 1e-12))
            # Double all bins except DC (and Nyquist bin if it were included)
            if half > 1:
                magnitudes[1:] *= 2.0
            phases = np.degrees(np.angle(fft_result[:half]))
            filtered_magnitudes = magnitudes[freq_mask]
            filtered_phases = phases[freq_mask]

            if self.settings.weighting_mode != "Linear":
                weights = np.ones_like(filtered_frequencies)
                if self.settings.weighting_mode == "A-Weighting":
                    weights = 1.0 / (1.0 + (filtered_frequencies / 1000) ** 2)
                elif self.settings.weighting_mode == "B-Weighting":
                    weights = 1.0 / (1.0 + (filtered_frequencies / 500) ** 2)
                elif self.settings.weighting_mode == "C-Weighting":
                    weights = 1.0 / (1.0 + (filtered_frequencies / 200) ** 2)
                filtered_magnitudes *= weights

            if self.settings.averaging_mode == "Linear" and len(self.data_buffer) > 1:
                avg_magnitudes = []
                avg_phases = []
                for d in self.data_buffer:
                    d_len = len(d)
                    w = window if len(window) == d_len else get_window(window_name, d_len) if window_name != "rectangular" else np.ones(d_len)
                    pd_len = 2 ** int(np.ceil(np.log2(d_len)))
                    d_pad = np.pad(d * w, (0, pd_len - d_len))
                    F = fft(d_pad)
                    h = pd_len // 2
                    cg_d = np.mean(w)
                    mags = np.abs(F[:h]) / (pd_len * max(cg_d, 1e-12))
                    if h > 1:
                        mags[1:] *= 2.0
                    phs = np.degrees(np.angle(F[:h]))
                    avg_magnitudes.append(mags)
                    avg_phases.append(phs)
                avg_magnitudes = np.mean(np.stack(avg_magnitudes, axis=0), axis=0)
                avg_phases = np.mean(np.stack(avg_phases, axis=0), axis=0)
                filtered_magnitudes = avg_magnitudes[freq_mask]
                filtered_phases = avg_phases[freq_mask]
            elif self.settings.averaging_mode == "Exponential" and len(self.data_buffer) > 1:
                alpha = 2.0 / (self.settings.number_of_averages + 1)
                avg_magnitudes = np.zeros(half)
                avg_phases = np.zeros(half)
                for d in self.data_buffer:
                    d_len = len(d)
                    w = window if len(window) == d_len else get_window(window_name, d_len) if window_name != "rectangular" else np.ones(d_len)
                    d_pad = np.pad(d * w, (0, target_length - d_len))
                    Fd = fft(d_pad)
                    mags = np.abs(Fd[:half]) / (target_length * max(np.mean(w), 1e-12))
                    if half > 1:
                        mags[1:] *= 2.0
                    phs = np.degrees(np.angle(Fd[:half]))
                    avg_magnitudes = alpha * mags + (1 - alpha) * avg_magnitudes
                    avg_phases = alpha * phs + (1 - alpha) * avg_phases
                filtered_magnitudes = avg_magnitudes[freq_mask]
                filtered_phases = avg_phases[freq_mask]

            if len(filtered_frequencies) > self.settings.number_of_lines:
                indices = np.linspace(0, len(filtered_frequencies) - 1, self.settings.number_of_lines, dtype=int)
                filtered_frequencies = filtered_frequencies[indices]
                filtered_magnitudes = filtered_magnitudes[indices]
                filtered_phases = filtered_phases[indices]

            self.magnitude_plot_item.setData(filtered_frequencies, filtered_magnitudes)
            self.phase_plot_item.setData(filtered_frequencies, filtered_phases)
            self.magnitude_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)
            self.phase_plot_widget.setXRange(self.settings.start_frequency, self.settings.stop_frequency, padding=0.02)

            if self.console:
                self.console.append_to_console(
                    f"FFT Updated: Samples={n}, FFT Size={target_length}, "
                    f"Fs={self.sample_rate}Hz, Lines={len(filtered_frequencies)}, "
                    f"Range={self.settings.start_frequency}-{self.settings.stop_frequency}Hz"
                )
        except Exception as e:
            self.log_and_set_status(f"Error updating FFT: {str(e)}")

    def log_and_set_status(self, message):
        logging.error(message)
        if self.console:
            self.console.append_to_console(message)

    def close(self):
        self.update_timer.stop()

    def cleanup(self):
        self.close()

    def refresh_channel_properties(self):
        self.initialize_async()

    # NEW: Load selected saved frame payload and plot FFT (first main channel by default if no explicit channel)
    def load_selected_frame(self, payload: dict):
        try:
            if not payload:
                self.log_and_set_status("FFT: Invalid selection payload (empty).")
                return
            num_main = int(payload.get("numberOfChannels", 0))
            num_tacho = int(payload.get("tacoChannelCount", 0))
            total_ch = num_main + num_tacho
            Fs = float(payload.get("samplingRate", 0) or 0)
            N = int(payload.get("samplingSize", 0) or 0)
            data_flat = payload.get("message", [])
            if not Fs or not N or not total_ch or not data_flat:
                self.log_and_set_status("FFT: Incomplete selection payload (Fs/N/channels/data missing).")
                return

            # Shape data into channels if flattened
            if isinstance(data_flat, list) and data_flat and isinstance(data_flat[0], (int, float)):
                if len(data_flat) != total_ch * N:
                    self.log_and_set_status(f"FFT: Data length mismatch. expected {total_ch*N}, got {len(data_flat)}")
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
                    self.log_and_set_status("FFT: Invalid nested data shape in selection payload.")
                    return

            # Choose channel index
            ch_idx = self.channel_index if self.channel_index is not None else 0
            if ch_idx >= len(values):
                self.log_and_set_status(f"FFT: Selected channel index {ch_idx} out of range for {len(values)} channels.")
                return

            # Counts -> volts
            raw = np.array(values[ch_idx][:self.max_samples], dtype=np.float64)
            volts = (raw - self.off_set) * self.scaling_factor
            # Calibrate & unit convert
            name_sf = self.channel_names[ch_idx] if 0 <= ch_idx < len(self.channel_names) else None
            props_sf = self.channel_properties.get(name_sf or str(ch_idx), {
                "unit": "mil", "correctionValue": 1.0, "gain": 1.0, "sensitivity": 1.0
            })
            try:
                base_value_sf = volts * (props_sf.get("correctionValue", 1.0) * props_sf.get("gain", 1.0)) / max(props_sf.get("sensitivity", 1.0), 1e-12)
            except Exception:
                base_value_sf = volts
            unit_sf = (props_sf.get("unit", "mil") or "mil").lower()
            if unit_sf == "mil":
                calibrated_sf = base_value_sf / 25.4
            elif unit_sf == "um":
                calibrated_sf = base_value_sf
            elif unit_sf == "mm":
                calibrated_sf = base_value_sf / 1000.0
            else:
                calibrated_sf = base_value_sf
            if self._y_unit_label != unit_sf:
                self._y_unit_label = unit_sf
                self.magnitude_plot_widget.setLabel('left', f'Amplitude ({unit_sf})', color='#000000')

            # Buffer and plot
            self.sample_rate = Fs
            self.latest_data = calibrated_sf.astype(np.float64)
            self.data_buffer = [self.latest_data.copy()]
            self.update_plot()
            if self.console:
                self.console.append_to_console(f"FFT: Loaded selected frame {payload.get('frameIndex')} ({N} samples @ {Fs}Hz)")
        except Exception as e:
            self.log_and_set_status(f"FFT: Error loading selected frame: {e}")

