import numpy as np
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QScrollArea, QPushButton, QComboBox, QGridLayout
from PyQt5.QtCore import QObject, QEvent, Qt, QTimer
from PyQt5.QtGui import QIcon, QFont
from pyqtgraph import PlotWidget, mkPen, AxisItem, SignalProxy, InfiniteLine
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TimeAxisItem(AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def tickStrings(self, values, scale, spacing):
        # Render ticks as two lines: MMDDYYYY on first, HH:MM::SS:CC on second (CC = centiseconds)
        labels = []
        for v in values:
            try:
                if isinstance(v, (int, float)) and v > 0:
                    dt = datetime.fromtimestamp(v)
                    centi = int(round(dt.microsecond / 10000.0))
                    # Date as MMDDYYYY without separators
                    date_str = dt.strftime('%m-%d-%Y')
                    # Time as HH:MM::SS:CC (with double colon before seconds)
                    hhmm = dt.strftime('%H:%M')
                    ss = dt.strftime('%S')
                    time_str = f"{hhmm}:{ss}:{centi:02d}"
                    labels.append(f"{date_str}\n{time_str}")
                else:
                    labels.append("")
            except Exception:
                labels.append("")
        return labels

class LeftAxisItem(AxisItem):
    def __init__(self, *args, decimals=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.decimals = decimals

    def tickStrings(self, values, scale, spacing):
        # Format numeric ticks with a fixed number of decimals if specified
        labels = []
        for v in values:
            try:
                if isinstance(v, (int, float)):
                    if self.decimals is not None:
                        labels.append(f"{v:.{self.decimals}f}")
                    else:
                        labels.append(f"{v}")
                else:
                    labels.append("")
            except Exception:
                labels.append("")
        return labels

class MouseTracker(QObject):
    def __init__(self, parent, idx, feature):
        super().__init__(parent)
        self.idx = idx
        self.feature = feature

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Enter:
            self.feature.mouse_enter(self.idx)
        elif event.type() == QEvent.Leave:
            self.feature.mouse_leave(self.idx)
        return False

class TimeViewFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None):
        super().__init__()
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.channel = channel
        self.model_name = model_name
        self.console = console

        self.widget = None
        self.plot_widgets = []
        self.plots = []
        self.fifo_data = []
        self.fifo_times = []
        self.vlines = []
        self.proxies = []
        self.trackers = []

        self.sample_rate = None
        self.main_channels = None
        self.tacho_channels_count = 2
        self.total_channels = None
        self.scaling_factor = 3.3 / 65535
        self.off_set=32768
        self.num_plots = None
        self.samples_per_channel = None
        self.window_seconds = 1
        self.previous_window_seconds = 1
        self.fifo_window_samples = None

        self.settings_panel = None
        self.settings_button = None
        self.refresh_timer = None
        self.needs_refresh = []
        self.is_initialized = False

        self.channel_properties = {}
        self.channel_names = []
        self.is_scrolling = False
        self.active_line_idx = None

        self.plot_colors = [
            '#0000FF', '#FF0000', '#00FF00', '#800080', '#FFA500', '#A52A2A', "#C21532", '#008080',
            '#FF4500', '#32CD32', '#00CED1', "#0D0D0C", '#FF69B4', '#8A2BE2', '#FF6347', '#20B2AA',
            '#ADFF2F', '#9932CC', '#FF7F50', '#00FA9A', '#9400D3'
        ]

        self.initUI()
        self.load_channel_properties()

    def initUI(self):
        self.widget = QWidget()
        main_layout = QVBoxLayout()

        header = QLabel(f"TIME VIEW")
        header.setStyleSheet("color: black; font-size: 26px; font-weight: bold; padding: 8px;")

        top_layout = QHBoxLayout()
        # Keep header centered with stretches on both sides and place settings button on the right
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
        top_layout.addWidget(header, alignment=Qt.AlignCenter)
        top_layout.addStretch()
        top_layout.addWidget(self.settings_button)
        main_layout.addLayout(top_layout)

        # Right sidebar settings panel (hidden by default)
        self.settings_panel = QWidget()
        self.settings_panel.setStyleSheet("""
        QWidget {
            background-color: #f5f5f5;
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            padding: 10px;
        }
        QLabel#settingsTitle { font-size: 16px; font-weight: 700; padding: 4px 0 10px 0; }
        """)
        self.settings_panel.setVisible(False)
        self.settings_panel.setFixedWidth(350)

        settings_layout = QGridLayout()
        settings_layout.setSpacing(10)
        self.settings_panel.setLayout(settings_layout)

        title = QLabel("Time View Settings")
        title.setObjectName("settingsTitle")
        settings_layout.addWidget(title, 0, 0, 1, 2)

        window_label = QLabel("Window Seconds")
        window_label.setStyleSheet("font-size: 14px;")
        # Put label above the dropdown spanning both columns
        settings_layout.addWidget(window_label, 1, 0, 1, 2)

        window_combo = QComboBox()
        window_combo.addItems([str(i) for i in range(1, 11)])
        window_combo.setCurrentText(str(self.window_seconds))
        window_combo.setStyleSheet("""
        QComboBox {
            padding: 6px 8px;
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            background-color: white;
            min-width: 120px;
        }
        """)
        # Dropdown sits below the label spanning both columns
        settings_layout.addWidget(window_combo, 2, 0, 1, 2)
        self.settings_widgets = {"WindowSeconds": window_combo}

        ok_button = QPushButton("OK")
        ok_button.setStyleSheet("""
        QPushButton {
            background-color: #2196F3;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 14px;
            min-width: 90px;
        }
        QPushButton:hover { background-color: #1e88e5; }
        QPushButton:pressed { background-color: #1976d2; }
        """)
        ok_button.clicked.connect(self.save_settings)

        cancel_button = QPushButton("Cancel")
        cancel_button.setStyleSheet("""
        QPushButton {
            background-color: #9e9e9e;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 14px;
            min-width: 90px;
        }
        QPushButton:hover { background-color: #8d8d8d; }
        QPushButton:pressed { background-color: #7b7b7b; }
        """)
        cancel_button.clicked.connect(self.close_settings)

        # Push buttons to the bottom
        settings_layout.setRowStretch(3, 1)
        settings_layout.addWidget(ok_button, 4, 0)
        settings_layout.addWidget(cancel_button, 4, 1)

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("""
            QScrollArea {
                border-radius: 8px;
                padding: 5px;
            }
            QScrollBar:vertical {
                background: white;
                width: 25px;
                margin: 0px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical {
                background: black;
                min-height: 60px;
                border-radius: 2px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
                background: none;
            }
        """)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        # Add vertical spacing and outer margins between stacked plots for visual separation
        self.scroll_layout.setSpacing(24)
        self.scroll_layout.setContentsMargins(10, 10, 10, 14)
        self.scroll_content.setStyleSheet("background-color: #ebeef2; border-radius: 5px; padding: 10px;")
        self.scroll_area.setWidget(self.scroll_content)

        # Content area: plots on the left, settings sidebar on the right
        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(8)
        content_layout.addWidget(self.scroll_area, 1)
        content_layout.addWidget(self.settings_panel)
        main_layout.addLayout(content_layout)

        self.widget.setLayout(main_layout)

        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.refresh_plots)
        self.refresh_timer.start(100)

        self.scroll_debounce_timer = QTimer()
        self.scroll_debounce_timer.setInterval(200)
        self.scroll_debounce_timer.timeout.connect(self.stop_scrolling)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self.on_scroll_changed)

        if not self.model_name and self.console:
            self.console.append_to_console("No model selected in TimeViewFeature.")
        if not self.channel and self.console:
            self.console.append_to_console("No channel selected in TimeViewFeature.")
        logging.debug("UI initialized, waiting for data to start plotting")

    def load_channel_properties(self):
        try:
            project_data = self.db.get_project_data(self.project_name)
            if not project_data:
                self.log_and_set_status(f"Project {self.project_name} not found")
                return
            for model in project_data.get("models", []):
                if model.get("name") == self.model_name:
                    self.channel_names = [ch.get("channelName") for ch in model.get("channels", [])]
                    for channel in model.get("channels", []):
                        channel_name = channel.get("channelName")
                        self.channel_properties[channel_name] = {
                            "type": channel.get("type", "Displacement"),
                            "unit": channel.get("unit", "mil").lower(),
                            "correctionValue": float(channel.get("correctionValue", "1.0") or "1.0"),
                            "gain": float(channel.get("gain", "1.0") or "1.0"),
                            "sensitivity": float(channel.get("sensitivity", "1.0") or "1.0"),
                            "convertedSensitivity": float(channel.get("ConvertedSensitivity", channel.get("sensitivity", "1.0")) or "1.0")
                        }
                    break
            logging.debug(f"Loaded channel properties: {self.channel_properties}")
            logging.debug(f"Channel names: {self.channel_names}")
        except Exception as e:
            self.log_and_set_status(f"Error loading channel properties: {str(e)}")

    def on_scroll_changed(self):
        self.is_scrolling = True
        self.scroll_debounce_timer.stop()
        self.scroll_debounce_timer.start()

    def stop_scrolling(self):
        self.is_scrolling = False
        self.scroll_debounce_timer.stop()

    def initialize_plots(self, channel_count):
        if not channel_count:
            self.log_and_set_status("Cannot initialize plots: channel count not set")
            return

        self.plot_widgets = []
        self.plots = []
        self.fifo_data = []
        self.fifo_times = []
        self.vlines = []
        self.proxies = []
        self.trackers = []
        self.needs_refresh = []

        self.num_plots = channel_count
        self.total_channels = channel_count
        self.main_channels = channel_count - self.tacho_channels_count

        for i in range(self.num_plots):
            # Wrap each plot in its own container to create consistent visual gaps
            plot_container = QWidget()
            plot_container.setStyleSheet("background-color: #ebeef2; border-radius: 6px;")
            container_layout = QVBoxLayout(plot_container)
            container_layout.setContentsMargins(6, 6, 6, 12)
            container_layout.setSpacing(4)

            plot_widget = PlotWidget()
            # Increased plot height for better visibility
            plot_widget.setMinimumHeight(280)
            plot_widget.setBackground('#ebeef2')
            plot_widget.showGrid(x=True, y=True)
            plot_widget.addLegend()

            axis = TimeAxisItem(orientation='bottom')
            # Choose left axis formatter: main channels by unit, frequency with 2 decimals, trigger default
            ch_name_for_axis = self.channel_names[i] if i < len(self.channel_names) else f"Channel {i + 1}"
            unit_for_axis = self.channel_properties.get(ch_name_for_axis, {}).get("unit", "mil")
            unit_for_axis_l = str(unit_for_axis).lower()
            if i < self.main_channels:
                if unit_for_axis_l == 'mm':
                    left_axis = LeftAxisItem(orientation='left', decimals=3)
                elif unit_for_axis_l == 'mil':
                    left_axis = LeftAxisItem(orientation='left', decimals=1)
                elif unit_for_axis_l == 'um':
                    left_axis = LeftAxisItem(orientation='left', decimals=0)
                elif unit_for_axis_l == 'v':
                    left_axis = LeftAxisItem(orientation='left', decimals=3)
                else:
                    left_axis = LeftAxisItem(orientation='left', decimals=None)
            else:
                # Special channels
                if i == self.main_channels:
                    # Frequency axis: always 2 decimals
                    left_axis = LeftAxisItem(orientation='left', decimals=1)
                else:
                    # Trigger: default formatting
                    left_axis = LeftAxisItem(orientation='left', decimals=1)
            plot_widget.setAxisItems({'bottom': axis, 'left': left_axis})
            # Axis styling: bold ticks and larger fonts for readability
            try:
                tick_font = QFont()
                tick_font.setPointSize(6)
                tick_font.setBold(True)
                for ax_name in ('bottom', 'left'):
                    ax = plot_widget.getAxis(ax_name)
                    ax.setStyle(tickFont=tick_font, tickTextOffset=6)
                    ax.setPen(mkPen(color='#000000', width=1))
                    ax.setTextPen(mkPen(color='#000000'))
            except Exception:
                pass
            # Provide more space for multi-line tick labels and improve readability
            try:
                plot_item = plot_widget.getPlotItem()
                plot_item.layout.setContentsMargins(12, 8, 12, 36)
                plot_widget.getAxis('bottom').setHeight(46)
                # Slightly increase tick text offset
                plot_widget.getAxis('bottom').setStyle(tickTextOffset=20)
            except Exception:
                pass
            container_layout.addWidget(plot_widget)
            self.scroll_layout.addWidget(plot_container)

            channel_name = self.channel_names[i] if i < len(self.channel_names) else f"Channel {i + 1}"
            unit = self.channel_properties.get(channel_name, {}).get("unit", "mil")
            y_label = f"{unit}" if i < self.main_channels else "Value"

            if i < self.main_channels:
                # Set label for main channels (bold and larger size)
                plot_widget.getAxis('left').setLabel(f"<b>{channel_name} ({y_label})</b>", **{'size': '13pt'})
            else:
                # Special channels: Frequency and Trigger
                if i == self.main_channels:
                    # Frequency axis: always 2 decimals
                    left_axis = LeftAxisItem(orientation='left', decimals=1)
                    plot_widget.getAxis('left').setLabel("<b>Frequency (Hz)</b>", **{'size': '13pt'})
                else:
                    # Trigger: default formatting
                    left_axis = LeftAxisItem(orientation='left', decimals=None)
                    plot_widget.getAxis('left').setLabel("<b>Trigger (0–1)</b>", **{'size': '13pt'})
                    plot_widget.setYRange(-0.1, 1.1, padding=0)

            plot = plot_widget.plot([], [], pen=mkPen(color=self.plot_colors[i % len(self.plot_colors)], width=1))
            # Enable performance optimizations
            try:
                plot.setDownsampling(auto=True)
                plot.setClipToView(True)
            except Exception:
                pass
            self.plot_widgets.append(plot_widget)
            self.plots.append(plot)

            self.fifo_data.append([])
            self.fifo_times.append([])
            self.needs_refresh.append(True)

            self.scroll_layout.addWidget(plot_widget)

            vline = InfiniteLine(pos=0, angle=90, movable=False, pen=mkPen('k', width=1, style=Qt.DashLine))
            vline.setVisible(False)
            plot_widget.addItem(vline)
            self.vlines.append(vline)

            tracker = MouseTracker(plot_widget, i, self)
            plot_widget.installEventFilter(tracker)
            self.trackers.append(tracker)

            proxy = SignalProxy(plot_widget.scene().sigMouseMoved, rateLimit=60, slot=lambda evt, idx=i: self.mouse_moved(evt, idx))
            self.proxies.append(proxy)

        self.scroll_area.setWidget(self.scroll_content)
        self.initialize_buffers()
        logging.debug(f"Initialized {self.num_plots} plots with {self.window_seconds}-second window")

    def initialize_buffers(self):
        if not self.sample_rate or not self.num_plots:
            self.log_and_set_status("Cannot initialize buffers: sample_rate or num_plots not set")
            return

        self.fifo_window_samples = int(self.sample_rate * self.window_seconds)
        current_time = datetime.now()
        time_step = 1.0 / self.sample_rate

        for i in range(self.num_plots):
            self.fifo_data[i] = np.zeros(self.fifo_window_samples)
            self.fifo_times[i] = np.array([current_time - timedelta(seconds=(self.fifo_window_samples - 1 - j) * time_step) for j in range(self.fifo_window_samples)])
            self.needs_refresh[i] = True

        self.is_initialized = True
        logging.debug(f"Initialized FIFO buffers: {self.num_plots} channels, {self.fifo_window_samples} samples each")

    def toggle_settings(self):
        self.settings_panel.setVisible(not self.settings_panel.isVisible())
        self.settings_button.setVisible(not self.settings_panel.isVisible())

    def save_settings(self):
        try:
            selected_seconds = int(self.settings_widgets["WindowSeconds"].currentText())
            if 1 <= selected_seconds <= 10:
                self.window_seconds = selected_seconds
                self.update_window_size()
                self.log_and_set_status(f"Applied window size: {self.window_seconds} seconds")
                self.refresh_plots()
            else:
                self.log_and_set_status(f"Invalid window seconds selected: {selected_seconds}. Must be 1-10.")
            self.settings_panel.setVisible(False)
            self.settings_button.setVisible(True)
        except Exception as e:
            self.log_and_set_status(f"Error saving TimeView settings: {str(e)}")

    def close_settings(self):
        self.settings_widgets["WindowSeconds"].setCurrentText(str(self.window_seconds))
        self.settings_panel.setVisible(False)
        self.settings_button.setVisible(True)

    def update_window_size(self):
        if not self.sample_rate or not self.num_plots or not self.is_initialized:
            self.log_and_set_status("Cannot update window size: sample_rate, num_plots, or initialization not set")
            return

        if self.window_seconds == self.previous_window_seconds:
            logging.debug("No change in window size, skipping update")
            return

        new_fifo_window_samples = int(self.sample_rate * self.window_seconds)
        current_time = datetime.now()
        time_step = 1.0 / self.sample_rate

        for i in range(self.num_plots):
            current_data = self.fifo_data[i]
            current_times = self.fifo_times[i]
            new_data = np.zeros(new_fifo_window_samples)
            new_times = np.array([current_time - timedelta(seconds=(new_fifo_window_samples - 1 - j) * time_step) for j in range(new_fifo_window_samples)])

            copy_length = min(len(current_data), new_fifo_window_samples)
            if copy_length > 0:
                new_data[-copy_length:] = current_data[-copy_length:]
                new_times[-copy_length:] = current_times[-copy_length:] if len(current_times) >= copy_length else new_times[-copy_length:]

            self.fifo_data[i] = new_data
            self.fifo_times[i] = new_times
            self.needs_refresh[i] = True

        self.fifo_window_samples = new_fifo_window_samples
        self.previous_window_seconds = self.window_seconds
        logging.debug(f"Updated FIFO buffers to {self.window_seconds} seconds, {self.fifo_window_samples} samples")
        # Do NOT reinitialize plots here; that would clear existing buffers and lose continuity.
        # Existing plots will render the resized buffers on the next refresh.
        self.refresh_plots()

    def get_widget(self):
        return self.widget

    def on_data_received(self, tag_name, model_name, values, sample_rate, frame_index):
        logging.debug(f"on_data_received called with tag_name={tag_name}, model_name={model_name}, values_len={len(values) if values else 0}, sample_rate={sample_rate}, frame_index={frame_index}")
        if self.model_name != model_name:
            logging.debug(f"Ignoring data for model {model_name}, expected {self.model_name}")
            return
        try:
            if not values or not sample_rate or sample_rate <= 0:
                self.log_and_set_status(f"Invalid MQTT data: values={values}, sample_rate={sample_rate}")
                return

            expected_channels = len(values)
            if self.main_channels is None:
                self.main_channels = expected_channels - self.tacho_channels_count
                if self.main_channels < 0:
                    self.log_and_set_status(f"Channel mismatch: received {expected_channels}, expected at least {self.tacho_channels_count} tacho channels")
                    return

            self.total_channels = expected_channels
            self.sample_rate = sample_rate
            self.samples_per_channel = len(values[0]) if values else 0

            if not all(len(values[i]) == self.samples_per_channel for i in range(expected_channels)):
                self.log_and_set_status(f"Channel data length mismatch: expected {self.samples_per_channel} samples")
                return

            if not self.is_initialized or len(self.fifo_data) != self.total_channels:
                self.num_plots = self.total_channels
                self.initialize_plots(self.total_channels)

            time_step = 1.0 / sample_rate

            for ch in range(self.total_channels):
                channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch + 1}"
                props = self.channel_properties.get(channel_name, {
                    "type": "Displacement",
                    "unit": "mil",
                    "correctionValue": 1.0,
                    "gain": 1.0,
                    "sensitivity": 1.0,
                    "convertedSensitivity": 1.0
                })
                volts = (np.array(values[ch]) - self.off_set) * self.scaling_factor
                # Build continuous timestamps by extending from previous last timestamp if available
                if isinstance(self.fifo_times[ch], np.ndarray) and self.fifo_times[ch].size > 0:
                    last_time = self.fifo_times[ch][-1]
                    # Ensure last_time is datetime
                    try:
                        base_time = last_time if isinstance(last_time, datetime) else datetime.fromtimestamp(float(last_time))
                    except Exception:
                        base_time = datetime.now()
                    new_times = np.array([base_time + timedelta(seconds=(i + 1) * time_step) for i in range(self.samples_per_channel)])
                else:
                    # First fill: anchor to now and backfill
                    current_time = datetime.now()
                    new_times = np.array([current_time - timedelta(seconds=(self.samples_per_channel - 1 - i) * time_step) for i in range(self.samples_per_channel)])
                # new_data = volts

                if ch < self.main_channels:
                    base_value = volts * (props["correctionValue"] * props["gain"]) / max(props["sensitivity"], 1e-12)
                    new_data = base_value
                elif ch == self.main_channels:
                    # Frequency channel: use payload values and scale by /100
                    new_data = np.ceil(np.array(values[ch], dtype=np.float64) / 100.0)
                else:
                    # Trigger channel: use payload directly and clamp to 0..1
                    new_data = np.clip(np.array(values[ch], dtype=np.float64), 0.0, 1.0)

                if len(self.fifo_data[ch]) != self.fifo_window_samples:
                    self.fifo_data[ch] = np.zeros(self.fifo_window_samples)
                    # Initialize time buffer so that it ends at the last new_times value
                    end_time = new_times[-1] if isinstance(new_times[-1], datetime) else datetime.now()
                    self.fifo_times[ch] = np.array([end_time - timedelta(seconds=(self.fifo_window_samples - 1 - j) * time_step) for j in range(self.fifo_window_samples)])

                self.fifo_data[ch] = np.roll(self.fifo_data[ch], -self.samples_per_channel)
                self.fifo_data[ch][-self.samples_per_channel:] = new_data
                self.fifo_times[ch] = np.roll(self.fifo_times[ch], -self.samples_per_channel)
                self.fifo_times[ch][-self.samples_per_channel:] = new_times
                self.needs_refresh[ch] = True

            # Do not sort the time arrays each update; rolling maintains chronological order
            # Simply mark channels for refresh (already set during update)

            self.refresh_plots()
        except Exception as e:
            logging.error(f"Error processing data: {str(e)}")
            self.log_and_set_status(f"Error processing data: {str(e)}")

    def refresh_plots(self):
        # Skip refresh until plots/buffers are initialized
        if self.is_scrolling or not self.is_initialized or not self.num_plots or self.num_plots <= 0:
            return
        try:
            # Compute a common time window [end - window_seconds, end] across all plots
            common_end_ts = None
            if self.num_plots and self.num_plots > 0:
                try:
                    ends = []
                    for i in range(int(self.num_plots)):
                        if isinstance(self.fifo_times[i], np.ndarray) and len(self.fifo_times[i]) > 0:
                            ends.append(self.fifo_times[i][-1].timestamp())
                    if ends:
                        common_end_ts = max(ends)
                except Exception:
                    common_end_ts = None

            for i in range(int(self.num_plots)):
                if not self.needs_refresh[i]:
                    continue
                if len(self.fifo_data[i]) == 0 or len(self.fifo_times[i]) == 0:
                    continue
                # Only update data on the existing PlotDataItem to avoid churn
                time_data = np.array([t.timestamp() for t in self.fifo_times[i]])
                self.plots[i].setData(time_data, self.fifo_data[i])
                if len(time_data) > 0:
                    if common_end_ts is not None and self.window_seconds:
                        x_max = common_end_ts
                        x_min = x_max - float(self.window_seconds)
                        self.plot_widgets[i].setXRange(x_min, x_max, padding=0.0)
                    else:
                        # Fallback to channel-local range with no padding
                        self.plot_widgets[i].setXRange(time_data.min(), time_data.max(), padding=0.0)
                # Y scaling: fixed for Trigger, auto for others
                if i == (self.main_channels + 1):
                    try:
                        self.plot_widgets[i].setYRange(-0.1, 1.1, padding=0)
                    except Exception:
                        pass
                else:
                    self.plot_widgets[i].enableAutoRange(axis='y')
                # Ensure sufficient space for two-line labels during refresh
                try:
                    self.plot_widgets[i].getAxis('bottom').setHeight(46)
                    self.plot_widgets[i].getPlotItem().layout.setContentsMargins(12, 8, 12, 36)
                    self.plot_widgets[i].getAxis('bottom').setStyle(tickTextOffset=20)
                except Exception:
                    pass
                self.needs_refresh[i] = False
        except Exception as e:
            logging.error(f"Error refreshing plots: {str(e)}")
            self.log_and_set_status(f"Error refreshing plots: {str(e)}")

    def load_file(self, filename):
        try:
            messages = self.db.get_history_messages(self.project_name, self.model_name, filename=filename)
            if not messages:
                self.log_and_set_status(f"No data found for filename {filename}")
                return

            message = messages[-1]
            main_channels = message.get("numberOfChannels", 0)
            tacho_channels = message.get("tacoChannelCount", 0)
            samples_per_channel = message.get("samplingSize", 0)
            sample_rate = message.get("samplingRate", 0)
            frame_index = message.get("frameIndex", 0)
            flattened_data = message.get("message", [])

            if not flattened_data or not sample_rate or not samples_per_channel:
                self.log_and_set_status(f"Invalid data in file {filename}")
                return

            total_channels = main_channels + tacho_channels
            if samples_per_channel * total_channels != len(flattened_data):
                self.log_and_set_status(f"Data length mismatch in file {filename}")
                return

            values = []
            for ch in range(total_channels):
                start_idx = ch * samples_per_channel
                end_idx = (ch + 1) * samples_per_channel
                values.append(flattened_data[start_idx:end_idx])

            self.main_channels = main_channels
            self.tacho_channels_count = tacho_channels
            self.total_channels = total_channels
            self.sample_rate = sample_rate
            self.samples_per_channel = samples_per_channel

            if not self.is_initialized or len(self.fifo_data) != self.total_channels:
                self.initialize_plots(total_channels)

            created_at = datetime.fromisoformat(message['createdAt'].replace('Z', '+00:00'))
            time_step = 1.0 / sample_rate
            new_times = np.array([created_at + timedelta(seconds=i * time_step) for i in range(samples_per_channel)])

            for ch in range(self.total_channels):
                channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch + 1}"
                props = self.channel_properties.get(channel_name, {
                    "type": "Displacement",
                    "unit": "mil",
                    "correctionValue": 1.0,
                    "gain": 1.0,
                    "sensitivity": 1.0,
                    "convertedSensitivity": 1.0
                })
                volts = (np.array(values[ch]) - self.off_set) * self.scaling_factor
                if ch < self.main_channels:
                    new_data = volts * (props["correctionValue"] * props["gain"]) / max(props["sensitivity"], 1e-12)
                elif ch == self.main_channels:
                    # Frequency: payload /100
                    new_data = np.array(values[ch], dtype=np.float64) / 100.0
                else:
                    # Trigger 0..1
                    new_data = np.clip(np.array(values[ch], dtype=np.float64), 0.0, 1.0)

                self.fifo_data[ch] = new_data
                self.fifo_times[ch] = new_times
                self.needs_refresh[ch] = True

            self.refresh_plots()
            if self.console:
                self.console.append_to_console(f"Loaded data from {filename}, frame {frame_index}")
        except Exception as e:
            logging.error(f"Error loading file {filename}: {str(e)}")
            self.log_and_set_status(f"Error loading file {filename}: {str(e)}")

    def load_selected_frame(self, payload: dict):
        try:
            main_channels = int(payload.get("numberOfChannels", 0))
            tacho_channels = int(payload.get("tacoChannelCount", 0))
            samples_per_channel = int(payload.get("samplingSize", 0))
            sample_rate = float(payload.get("samplingRate", 0))
            flattened_data = payload.get("channelData", [])
            created_at_str = payload.get("timestamp")

            if not flattened_data or not sample_rate or not samples_per_channel:
                self.log_and_set_status("Invalid payload: missing channelData/sample_rate/samples_per_channel")
                return

            total_channels = main_channels + tacho_channels
            if samples_per_channel * total_channels != len(flattened_data):
                self.log_and_set_status(f"Payload data length mismatch: expected {samples_per_channel * total_channels}, got {len(flattened_data)}")
                return

            values = []
            for ch in range(total_channels):
                start_idx = ch * samples_per_channel
                end_idx = (ch + 1) * samples_per_channel
                values.append(flattened_data[start_idx:end_idx])

            self.main_channels = main_channels
            self.tacho_channels_count = tacho_channels
            self.total_channels = total_channels
            self.sample_rate = sample_rate
            self.samples_per_channel = samples_per_channel

            if not self.is_initialized or len(self.fifo_data) != self.total_channels:
                self.initialize_plots(total_channels)

            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(str(created_at_str).replace('Z', '+00:00'))
                except Exception:
                    created_at = datetime.now()
            else:
                created_at = datetime.now()

            time_step = 1.0 / sample_rate
            new_times = np.array([created_at + timedelta(seconds=i * time_step) for i in range(samples_per_channel)])

            for ch in range(self.total_channels):
                channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch + 1}"
                props = self.channel_properties.get(channel_name, {
                    "type": "Displacement",
                    "unit": "mil",
                    "correctionValue": 1.0,
                    "gain": 1.0,
                    "sensitivity": 1.0,
                    "convertedSensitivity": 1.0
                })

                volts = (np.array(values[ch]) - self.off_set) * self.scaling_factor
                if ch < self.main_channels:
                    unit = (props.get("unit", "mil") or "mil").lower()
                    if unit == "v":
                        new_data = volts
                    else:
                        new_data = volts * (props["correctionValue"] * props["gain"]) * props["sensitivity"]
                elif ch == self.main_channels:
                    new_data = volts / 10
                else:
                    new_data = volts

                self.fifo_data[ch] = np.array(new_data)
                self.fifo_times[ch] = np.array(new_times)
                self.needs_refresh[ch] = True

            self.refresh_plots()
            if self.console:
                self.console.append_to_console(f"Loaded selected frame {payload.get('frameIndex')} from {payload.get('filename')}")
        except Exception as e:
            logging.error(f"Error loading selected frame: {str(e)}")
            self.log_and_set_status(f"Error loading selected frame: {str(e)}")

    def mouse_enter(self, idx):
        self.active_line_idx = idx
        self.vlines[idx].setVisible(True)

    def mouse_leave(self, idx):
        self.active_line_idx = None
        for vline in self.vlines:
            vline.setVisible(False)

    def mouse_moved(self, evt, idx):
        if self.active_line_idx is None:
            return
        pos = evt[0]
        if not self.plot_widgets[idx].sceneBoundingRect().contains(pos):
            return
        mouse_point = self.plot_widgets[idx].plotItem.vb.mapSceneToView(pos)
        x = mouse_point.x()
        times = self.fifo_times[idx]
        if len(times) > 0:
            time_stamps = np.array([t.timestamp() for t in times])
            if x < time_stamps[0]:
                x = time_stamps[0]
            elif x > time_stamps[-1]:
                x = time_stamps[-1]
            for vline in self.vlines:
                vline.setPos(x)
                vline.setVisible(True)

    def log_and_set_status(self, message):
        logging.error(message)
        if self.console:
            self.console.append_to_console(message)

    def cleanup(self):
        try:
            if self.refresh_timer.isActive():
                self.refresh_timer.stop()
            for plot in self.plots:
                plot.setData([], [])
            for widget in self.plot_widgets:
                widget.clear()
            while self.scroll_layout.count():
                item = self.scroll_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()
            self.plot_widgets = []
            self.plots = []
            self.fifo_data = []
            self.fifo_times = []
            self.vlines = []
            self.proxies = []
            self.trackers = []
            self.num_plots = 0
            if self.widget:
                self.widget.setParent(None)
                self.widget.deleteLater()
            logging.debug("TimeViewFeature cleaned up")
        except Exception as e:
            logging.error(f"Error during cleanup: {str(e)}")